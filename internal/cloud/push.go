// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"

	"golang.org/x/crypto/nacl/box"
)

const (
	pushKindRegister = "register"
	pushKindEvent    = "event"
)

var ErrPushDisabled = errors.New("push relay not configured")

type pushItem struct {
	Kind string          `json:"kind"`
	Body json.RawMessage `json:"body"`
	At   int64           `json:"at"`
}

type pushRelay struct {
	ctx      context.Context
	cfg      config.CloudConfig
	lim      config.Limits
	client   *http.Client
	pub      *[32]byte
	priv     *[32]byte
	wake     chan struct{}
	mu       sync.Mutex
	queueDir string
}

func newPushRelay(ctx context.Context, dir, keyPath string, cfg config.CloudConfig, lim config.Limits) (*pushRelay, error) {
	p := &pushRelay{
		ctx:      ctx,
		cfg:      cfg,
		lim:      lim,
		client:   &http.Client{Timeout: lim.PushHTTPTimeout.Std()},
		wake:     make(chan struct{}, 1),
		queueDir: filepath.Join(dir, "queue"),
	}
	if err := os.MkdirAll(p.queueDir, 0o700); err != nil {
		return nil, err
	}
	if err := p.loadOrCreateKey(keyPath); err != nil {
		return nil, err
	}
	if cfg.PushForwardURL != "" {
		go p.forwardLoop()
	}
	return p, nil
}

func (p *pushRelay) loadOrCreateKey(path string) error {
	raw, err := os.ReadFile(path)
	if err == nil {
		trimmed := strings.TrimSpace(string(raw))
		seed, derr := hex.DecodeString(trimmed)
		if derr != nil || len(seed) != 32 {
			return fmt.Errorf("push key %q: expected 64 hex characters", path)
		}
		var priv [32]byte
		copy(priv[:], seed)
		pub, prv, kerr := box.GenerateKey(bytes.NewReader(priv[:]))
		if kerr != nil {
			return kerr
		}
		p.pub, p.priv = pub, prv
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read push key %q: %w", path, err)
	}
	pub, prv, kerr := box.GenerateKey(rand.Reader)
	if kerr != nil {
		return kerr
	}
	if werr := os.WriteFile(path, []byte(hex.EncodeToString(prv[:])+"\n"), 0o600); werr != nil {
		return fmt.Errorf("write push key %q: %w", path, werr)
	}
	p.pub, p.priv = pub, prv
	return nil
}

func (p *pushRelay) publicKeyB64() string {
	if p.pub == nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(p.pub[:])
}

func (p *pushRelay) enabled() bool { return p.cfg.PushForwardURL != "" }

func (p *pushRelay) unseal(sealedB64, senderPubB64 string) ([]byte, error) {
	sealed, err := base64.StdEncoding.DecodeString(sealedB64)
	if err != nil || len(sealed) <= 24 {
		return nil, errors.New("sealed payload malformed")
	}
	senderPub, err := base64.StdEncoding.DecodeString(senderPubB64)
	if err != nil || len(senderPub) != 32 {
		return nil, errors.New("sender_box_pub malformed")
	}
	var nonce [24]byte
	copy(nonce[:], sealed[:24])
	var peerPub [32]byte
	copy(peerPub[:], senderPub)
	plain, ok := box.Open(nil, sealed[24:], &nonce, &peerPub, p.priv)
	if !ok {
		return nil, errors.New("sealed payload could not be opened")
	}
	return plain, nil
}

func (p *pushRelay) enqueue(kind string, body []byte) error {
	if !p.enabled() {
		return ErrPushDisabled
	}
	item := pushItem{Kind: kind, Body: body, At: time.Now().UnixMilli()}
	raw, err := json.Marshal(item)
	if err != nil {
		return err
	}
	var suffix [6]byte
	_, _ = rand.Read(suffix[:])
	name := strconv.FormatInt(item.At, 10) + "-" + hex.EncodeToString(suffix[:]) + ".json"
	p.mu.Lock()
	err = writeAtomic(filepath.Join(p.queueDir, name), raw)
	p.mu.Unlock()
	if err != nil {
		return err
	}
	select {
	case p.wake <- struct{}{}:
	default:
	}
	return nil
}

func (p *pushRelay) queueLen() int {
	entries := listDir(p.queueDir)
	n := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".json") {
			n++
		}
	}
	return n
}

func (p *pushRelay) forwardLoop() {
	backoff := time.Second
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-p.wake:
		case <-time.After(backoff):
		}
		ok := p.drain()
		if ok {
			backoff = time.Second
		} else {
			backoff *= 2
			if backoff > p.lim.PushMaxBackoff.Std() {
				backoff = p.lim.PushMaxBackoff.Std()
			}
		}
	}
}

func (p *pushRelay) drain() bool {
	entries := listDir(p.queueDir)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".json") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	allDone := true
	for _, name := range names {
		path := filepath.Join(p.queueDir, name)
		raw, err := os.ReadFile(path)
		if err != nil {
			slog.Warn("push relay: reading queued item failed", "file", name, "err", err)
			continue
		}
		var item pushItem
		if err := json.Unmarshal(raw, &item); err != nil {
			slog.Warn("push relay: dropping unreadable queued item", "file", name, "err", err)
			removeFile(path)
			continue
		}
		ttl := p.lim.PushEventTTL.Std()
		if item.Kind == pushKindRegister {
			ttl = p.lim.PushRegisterTTL.Std()
		}
		if time.Since(time.UnixMilli(item.At)) > ttl {
			removeFile(path)
			continue
		}
		reachable, done := p.forward(item)
		if !reachable {
			return false
		}
		if done {
			removeFile(path)
			continue
		}
		allDone = false
	}
	return allDone
}

func (p *pushRelay) forward(item pushItem) (reachable bool, done bool) {
	route := "/gw/front"
	if item.Kind == pushKindRegister {
		route = "/gw/register"
	}
	url := strings.TrimRight(p.cfg.PushForwardURL, "/") + route
	req, err := http.NewRequestWithContext(p.ctx, http.MethodPost, url, bytes.NewReader(item.Body))
	if err != nil {
		slog.Warn("push relay: building request failed, dropped", "kind", item.Kind, "err", err)
		return true, true
	}
	req.Header.Set("Content-Type", "application/json")
	if p.cfg.PushForwardToken != "" {
		req.Header.Set("Authorization", "Bearer "+p.cfg.PushForwardToken)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		slog.Warn("push relay: gateway unreachable", "err", err)
		return false, false
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	if resp.StatusCode >= 500 {
		slog.Warn("push relay: gateway error, retrying", "status", resp.StatusCode, "kind", item.Kind)
		return true, false
	}
	if resp.StatusCode >= 400 {
		slog.Warn("push relay: gateway rejected item, dropped", "kind", item.Kind, "status", resp.StatusCode)
	}
	return true, true
}
