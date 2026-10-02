// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"crypto/sha256"
	"encoding/hex"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/relay"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Inbox is the store-and-forward queue: one directory per recipient, one file
// per packet, named <arrival ms>-<packet id>.pkt so replay is oldest first and
// a repeated packet id lands on the same name.
type Inbox struct {
	dir string
	cfg config.CloudConfig

	mu    sync.Mutex
	locks map[peer.ID]*sync.Mutex

	// full reports whether the disk is at the watermark.
	full func() bool

	// Live packet count for /health. It used to be computed by walking every
	// recipient directory and stat-ing every file on EVERY request, and since
	// the apps call /health on every wake that walk grew with the inbox until a
	// single request took seconds and allocated hundreds of MB. Concurrent
	// requests then piled up and the node was OOM-killed (2026-09-14). Kept as
	// a counter instead, maintained on write and delete and reconciled by
	// sweep, which already walks the whole tree for its own reasons.
	pkts atomic.Int64
}

func newInbox(dir string, cfg config.CloudConfig) *Inbox {
	ib := &Inbox{dir: dir, cfg: cfg, locks: make(map[peer.ID]*sync.Mutex)}
	// One walk at startup to seed the counter. Bounded, off the request path,
	// and it drops anything already expired while it is in there.
	ib.sweep()
	return ib
}

func (ib *Inbox) lock(p peer.ID) func() {
	ib.mu.Lock()
	m, ok := ib.locks[p]
	if !ok {
		m = &sync.Mutex{}
		ib.locks[p] = m
	}
	ib.mu.Unlock()
	m.Lock()
	return m.Unlock
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
	entries, err := os.ReadDir(ib.recipientDir(p))
	if err != nil {
		return nil
	}
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

// Store keeps p for recipient. Returns false when it could not be stored.
func (ib *Inbox) Store(recipient peer.ID, p *relay.Packet) bool {
	raw, err := p.Marshal()
	if err != nil {
		return false
	}
	if int64(len(raw)) > ib.quota() {
		return false
	}
	if ib.full != nil && ib.full() {
		return false
	}
	unlock := ib.lock(recipient)
	defer unlock()
	dir := ib.recipientDir(recipient)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		log.Printf("inbox: create %s: %v", dir, err)
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
			_ = os.Remove(filepath.Join(dir, f.name))
			dropped++
			continue
		}
		total += f.size
		kept = append(kept, f)
	}
	for len(kept) > 0 && total+int64(len(raw)) > ib.quota() {
		oldest := kept[0]
		_ = os.Remove(filepath.Join(dir, oldest.name))
		dropped++
		total -= oldest.size
		kept = kept[1:]
	}
	name := strconv.FormatInt(time.Now().UnixMilli(), 10) + "-" + id
	if from != "" {
		name += "-" + from
	}
	name += ".pkt"
	if err := writeAtomic(filepath.Join(dir, name), raw); err != nil {
		log.Printf("inbox: store for %s: %v", recipient, err)
		ib.pkts.Add(-dropped)
		return false
	}
	ib.pkts.Add(1 - dropped)
	return true
}

// Replay hands every stored packet for recipient to deliver, oldest first, and
// removes each one deliver accepted. deliver returns false to stop (the socket
// is gone); what remains stays for the next connect.
func (ib *Inbox) Replay(recipient peer.ID, deliver func(*relay.Packet) bool) int {
	unlock := ib.lock(recipient)
	defer unlock()
	dir := ib.recipientDir(recipient)
	files := ib.list(recipient)
	cutoff := time.Now().Add(-ib.ttl())
	n := 0
	gone := int64(0)
	for _, f := range files {
		path := filepath.Join(dir, f.name)
		if f.at.Before(cutoff) {
			_ = os.Remove(path)
			gone++
			continue
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		p, err := relay.UnmarshalPacket(raw)
		if err != nil {
			_ = os.Remove(path)
			gone++
			continue
		}
		if !deliver(p) {
			break
		}
		_ = os.Remove(path)
		gone++
		n++
	}
	ib.pkts.Add(-gone)
	if len(ib.list(recipient)) == 0 {
		_ = os.Remove(dir)
	}
	return n
}

// Take returns and removes everything stored for recipient (the HTTP replay).
func (ib *Inbox) Take(recipient peer.ID) []*relay.Packet {
	var out []*relay.Packet
	ib.Replay(recipient, func(p *relay.Packet) bool {
		out = append(out, p)
		return true
	})
	return out
}

// count is O(1): see the note on Inbox.pkts. Never walk the tree here.
func (ib *Inbox) count() int {
	n := ib.pkts.Load()
	if n < 0 {
		return 0
	}
	return int(n)
}

// sweep drops expired packets and empty recipient directories. It walks the
// whole tree anyway, so it also recomputes the packet counter from what it
// actually sees: any drift from a missed path or a crash mid-write is corrected
// on the next pass rather than accumulating for ever.
func (ib *Inbox) sweep() {
	entries, err := os.ReadDir(ib.dir)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-ib.ttl())
	seen := int64(0)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		p, err := peer.Decode(e.Name())
		if err != nil {
			// Not a recipient directory we can lock and expire, but anything
			// in it is still on disk and still ours to report. Counting it
			// separately keeps the reconcile honest: tying the count to
			// whether the name decodes would silently zero out a stray
			// directory and leave the counter reading low for ever.
			seen += int64(countPkts(filepath.Join(ib.dir, e.Name())))
			continue
		}
		unlock := ib.lock(p)
		dir := ib.recipientDir(p)
		remaining := 0
		for _, f := range ib.list(p) {
			if f.at.Before(cutoff) {
				_ = os.Remove(filepath.Join(dir, f.name))
				continue
			}
			remaining++
		}
		if remaining == 0 {
			_ = os.Remove(dir)
		}
		seen += int64(remaining)
		unlock()
	}
	ib.pkts.Store(seen)
}

// countPkts counts packet files in one directory. Only used by sweep, which is
// off the request path; count() must never do this.
func countPkts(dir string) int {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	n := 0
	for _, f := range ents {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".pkt") {
			n++
		}
	}
	return n
}
