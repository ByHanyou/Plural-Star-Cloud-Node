// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
)

func newTestPush(t *testing.T, url string) *pushRelay {
	t.Helper()
	dir := t.TempDir()
	p, err := newPushRelay(context.Background(), dir, filepath.Join(dir, "push.key"), config.CloudConfig{}, config.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	p.cfg.PushForwardURL = url
	return p
}

func TestPushRelayDisabledWithoutURL(t *testing.T) {
	p := newTestPush(t, "")
	if p.enabled() {
		t.Fatal("relay enabled without a forward url")
	}
	if err := p.enqueue(pushKindEvent, []byte("{}")); !errors.Is(err, ErrPushDisabled) {
		t.Fatalf("enqueue error = %v, want ErrPushDisabled", err)
	}
	if p.publicKeyB64() == "" {
		t.Fatal("no push box public key")
	}
}

func TestPushRelayKeyPersists(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "push.key")
	a, err := newPushRelay(context.Background(), dir, keyPath, config.CloudConfig{}, config.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	b, err := newPushRelay(context.Background(), dir, keyPath, config.CloudConfig{}, config.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if a.publicKeyB64() != b.publicKeyB64() {
		t.Fatal("push key changed between loads")
	}
	if err := os.WriteFile(keyPath, []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newPushRelay(context.Background(), dir, keyPath, config.CloudConfig{}, config.DefaultLimits()); err == nil {
		t.Fatal("malformed key file accepted")
	}
}

func TestPushRelayDrainForwardsRegisterAndEvent(t *testing.T) {
	var mu sync.Mutex
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = append(got, r.URL.Path+" "+r.Header.Get("Authorization")+" "+r.Header.Get("Content-Type"))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	p := newTestPush(t, srv.URL)
	p.cfg.PushForwardToken = "tok"
	if err := p.enqueue(pushKindRegister, []byte(`{"k":"r"}`)); err != nil {
		t.Fatal(err)
	}
	if err := p.enqueue(pushKindEvent, []byte(`{"k":"e"}`)); err != nil {
		t.Fatal(err)
	}
	if p.queueLen() != 2 {
		t.Fatalf("queue = %d, want 2", p.queueLen())
	}
	if !p.drain() {
		t.Fatal("drain did not finish the queue")
	}
	if p.queueLen() != 0 {
		t.Fatalf("queue after drain = %d", p.queueLen())
	}
	mu.Lock()
	sort.Strings(got)
	mu.Unlock()
	want := []string{"/gw/front Bearer tok application/json", "/gw/register Bearer tok application/json"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("forwarded = %v, want %v", got, want)
	}
}

func TestPushRelayRetriesOn5xxAndDropsOn4xx(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusServiceUnavailable)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(int(status.Load()))
	}))
	defer srv.Close()
	p := newTestPush(t, srv.URL)
	if err := p.enqueue(pushKindEvent, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if p.drain() {
		t.Fatal("a 5xx answer counted as done")
	}
	if p.queueLen() != 1 {
		t.Fatalf("queue after 5xx = %d, want 1", p.queueLen())
	}
	status.Store(http.StatusBadRequest)
	if !p.drain() {
		t.Fatal("drain did not finish after the 4xx")
	}
	if p.queueLen() != 0 {
		t.Fatalf("a 4xx answer kept the item: queue = %d", p.queueLen())
	}
}

func TestPushRelayUnreachableKeepsQueue(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close()
	p := newTestPush(t, url)
	if err := p.enqueue(pushKindEvent, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if p.drain() {
		t.Fatal("unreachable gateway reported as drained")
	}
	if p.queueLen() != 1 {
		t.Fatalf("queue after failure = %d, want 1", p.queueLen())
	}
}

func TestPushRelayDropsExpiredItems(t *testing.T) {
	p := newTestPush(t, "http://127.0.0.1:1")
	item := pushItem{Kind: pushKindEvent, Body: json.RawMessage(`{}`), At: time.Now().Add(-2 * p.lim.PushEventTTL.Std()).UnixMilli()}
	raw, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p.queueDir, "0-old.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if !p.drain() {
		t.Fatal("expired item blocked the drain")
	}
	if p.queueLen() != 0 {
		t.Fatalf("expired item kept: queue = %d", p.queueLen())
	}
}
