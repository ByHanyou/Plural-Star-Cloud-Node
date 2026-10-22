// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/relay"

	"github.com/libp2p/go-libp2p/core/peer"
)

type Inbox struct {
	dir string
	cfg config.CloudConfig

	locks *keyedMutex[peer.ID]

	full func() bool

	pkts atomic.Int64
}

func newInbox(dir string, cfg config.CloudConfig) *Inbox {
	ib := &Inbox{dir: dir, cfg: cfg, locks: newKeyedMutex[peer.ID]()}
	ib.sweep()
	return ib
}

func (ib *Inbox) ttl() time.Duration { return time.Duration(ib.cfg.InboxTTLDays) * 24 * time.Hour }

func (ib *Inbox) quota() int64 { return int64(ib.cfg.InboxQuotaMB) << 20 }

func (ib *Inbox) recipientDir(p peer.ID) string { return filepath.Join(ib.dir, p.String()) }

type inboxFile struct {
	name string
	at   time.Time
	id   string
	from string
	size int64
}

func senderTag(sender []byte) string {
	if len(sender) == 0 {
		return ""
	}
	sum := sha256.Sum256(sender)
	return hex.EncodeToString(sum[:8])
}

func (ib *Inbox) list(p peer.ID) []inboxFile {
	entries := listDir(ib.recipientDir(p))
	var out []inboxFile
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".pkt") {
			continue
		}
		parts := strings.SplitN(strings.TrimSuffix(name, ".pkt"), "-", 3)
		if len(parts) < 2 {
			continue
		}
		from := ""
		if len(parts) == 3 {
			from = parts[2]
		}
		ms, perr := strconv.ParseInt(parts[0], 10, 64)
		if perr != nil {
			continue
		}
		info, ierr := e.Info()
		if ierr != nil {
			continue
		}
		out = append(out, inboxFile{name: name, at: time.UnixMilli(ms), id: parts[1], from: from, size: info.Size()})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].at.Equal(out[j].at) {
			return out[i].id < out[j].id
		}
		return out[i].at.Before(out[j].at)
	})
	return out
}

func (ib *Inbox) Store(recipient peer.ID, p *relay.Packet) bool {
	raw, err := p.Marshal()
	if err != nil {
		slog.Warn("inbox: encoding packet failed", "recipient", recipient, "err", err)
		return false
	}
	if int64(len(raw)) > ib.quota() {
		return false
	}
	if ib.full != nil && ib.full() {
		return false
	}
	unlock := ib.locks.lock(recipient)
	defer unlock()
	dir := ib.recipientDir(recipient)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		slog.Warn("inbox: create directory failed", "dir", dir, "err", err)
		return false
	}
	id := hex.EncodeToString(p.ID[:])
	from := senderTag(p.SenderID)
	files := ib.list(recipient)
	cutoff := time.Now().Add(-ib.ttl())
	var total int64
	dropped := int64(0)
	kept := files[:0]
	for _, f := range files {
		if f.id == id || f.at.Before(cutoff) || (from != "" && f.from == from) {
			if removeFile(filepath.Join(dir, f.name)) {
				dropped++
			}
			continue
		}
		total += f.size
		kept = append(kept, f)
	}
	for len(kept) > 0 && total+int64(len(raw)) > ib.quota() {
		oldest := kept[0]
		if removeFile(filepath.Join(dir, oldest.name)) {
			dropped++
		}
		total -= oldest.size
		kept = kept[1:]
	}
	name := strconv.FormatInt(time.Now().UnixMilli(), 10) + "-" + id
	if from != "" {
		name += "-" + from
	}
	name += ".pkt"
	if err := writeAtomic(filepath.Join(dir, name), raw); err != nil {
		slog.Warn("inbox: store failed", "recipient", recipient, "err", err)
		ib.pkts.Add(-dropped)
		return false
	}
	ib.pkts.Add(1 - dropped)
	return true
}

func (ib *Inbox) Replay(recipient peer.ID, deliver func(*relay.Packet) bool) int {
	unlock := ib.locks.lock(recipient)
	defer unlock()
	dir := ib.recipientDir(recipient)
	files := ib.list(recipient)
	cutoff := time.Now().Add(-ib.ttl())
	n := 0
	gone := int64(0)
	for _, f := range files {
		path := filepath.Join(dir, f.name)
		if f.at.Before(cutoff) {
			if removeFile(path) {
				gone++
			}
			continue
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			slog.Warn("inbox: reading packet failed", "file", path, "err", err)
			continue
		}
		p, err := relay.UnmarshalPacket(raw)
		if err != nil {
			slog.Warn("inbox: dropping unreadable packet", "file", path, "err", err)
			if removeFile(path) {
				gone++
			}
			continue
		}
		if !deliver(p) {
			break
		}
		if removeFile(path) {
			gone++
		}
		n++
	}
	ib.pkts.Add(-gone)
	if len(ib.list(recipient)) == 0 {
		removeFile(dir)
	}
	return n
}

func (ib *Inbox) Take(recipient peer.ID) []*relay.Packet {
	var out []*relay.Packet
	ib.Replay(recipient, func(p *relay.Packet) bool {
		out = append(out, p)
		return true
	})
	return out
}

func (ib *Inbox) count() int {
	n := ib.pkts.Load()
	if n < 0 {
		return 0
	}
	return int(n)
}

func (ib *Inbox) sweep() {
	entries := listDir(ib.dir)
	cutoff := time.Now().Add(-ib.ttl())
	seen := int64(0)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		p, err := peer.Decode(e.Name())
		if err != nil {
			seen += int64(countPkts(filepath.Join(ib.dir, e.Name())))
			continue
		}
		unlock := ib.locks.lock(p)
		dir := ib.recipientDir(p)
		remaining := 0
		for _, f := range ib.list(p) {
			if f.at.Before(cutoff) && removeFile(filepath.Join(dir, f.name)) {
				continue
			}
			remaining++
		}
		if remaining == 0 {
			removeFile(dir)
		}
		seen += int64(remaining)
		unlock()
	}
	ib.pkts.Store(seen)
}

func countPkts(dir string) int {
	ents := listDir(dir)
	n := 0
	for _, f := range ents {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".pkt") {
			n++
		}
	}
	return n
}
