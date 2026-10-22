// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
)

func (s *Service) gcLoop(ctx context.Context) {
	statePath := filepath.Join(s.root, "gc.json")
	last := readGCState(statePath)
	if time.Since(last) > 24*time.Hour {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Minute):
		}
		s.runGC()
		writeGCState(statePath, time.Now())
	}
	for {
		next := nextGCTime(time.Now().UTC(), s.cfg.GCHourUTC)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Until(next)):
			s.runGC()
			writeGCState(statePath, time.Now())
		}
	}
}

func nextGCTime(now time.Time, hour int) time.Time {
	next := time.Date(now.Year(), now.Month(), now.Day(), hour, 0, 0, 0, time.UTC)
	if !next.After(now) {
		next = next.Add(24 * time.Hour)
	}
	return next
}

func readGCState(path string) time.Time {
	raw, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.Warn("cloud: reading gc state failed", "path", path, "err", err)
		}
		return time.Time{}
	}
	ms, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		slog.Warn("cloud: gc state unreadable", "path", path, "err", err)
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

func writeGCState(path string, at time.Time) {
	if err := os.WriteFile(path, []byte(strconv.FormatInt(at.UnixMilli(), 10)), 0o600); err != nil {
		slog.Warn("cloud: saving gc state failed", "path", path, "err", err)
	}
}

func (s *Service) runGC() {
	start := time.Now()
	graceCutoff := start.Add(-time.Duration(s.cfg.GraceDays) * 24 * time.Hour)
	defer func() {
		s.objects.sweepParts(s.lim.GCObjectGrace.Std())
		s.inbox.sweep()
		s.fronts.sweep(s.lim.FrontMaxAge.Std())
	}()
	live, expired, err := s.vaults.liveObjects(graceCutoff)
	if err != nil {
		slog.Warn("gc: object collection skipped", "err", err)
		return
	}
	for _, id := range expired {
		if err := s.vaults.Delete(id); err != nil {
			slog.Warn("gc: delete vault failed", "vault", id, "err", err)
		}
	}
	if len(expired) > 0 {
		if live, _, err = s.vaults.liveObjects(graceCutoff); err != nil {
			slog.Warn("gc: object collection skipped", "err", err)
			return
		}
	}
	removed, freed := s.collectObjects(s.objects.local, live, start)
	if s.media.Name() != config.MediaBackendLocal {
		r, f := s.collectObjects(s.media, live, start)
		removed += r
		freed += f
	}
	slog.Info("gc: finished", "expired_vaults", len(expired), "removed_objects", removed, "freed_bytes", freed, "live_objects", len(live), "took", time.Since(start).Round(time.Millisecond))
}

func (s *Service) collectObjects(b Backend, live map[string]struct{}, now time.Time) (int, int64) {
	type doomedObject struct {
		id   string
		size int64
	}
	var doomed []doomedObject
	err := b.List(func(id string, size int64, modified time.Time) error {
		if _, ok := live[id]; ok {
			return nil
		}
		if now.Sub(modified) < s.lim.GCObjectGrace.Std() {
			return nil
		}
		doomed = append(doomed, doomedObject{id: id, size: size})
		return nil
	})
	if err != nil {
		slog.Warn("gc: list objects failed", "backend", b.Name(), "err", err)
		return 0, 0
	}
	removed := 0
	var freed int64
	for _, d := range doomed {
		if err := b.Delete(d.id); err != nil {
			slog.Warn("gc: delete object failed", "backend", b.Name(), "object", d.id, "err", err)
			continue
		}
		removed++
		freed += d.size
	}
	return removed, freed
}

func (v *vaultStore) liveObjects(graceCutoff time.Time) (live map[string]struct{}, expiredVaults []string, err error) {
	live = make(map[string]struct{})
	now := time.Now()
	for _, id := range v.ids() {
		unlock := v.locks.lock(id)
		m, rerr := v.readMeta(id)
		if rerr != nil {
			unlock()
			if errors.Is(rerr, ErrVaultMissing) {
				continue
			}
			return nil, nil, rerr
		}
		graceStart := m.GraceStartedAt
		if graceStart == 0 {
			graceStart = m.Updated
		}
		if len(m.Devices) == 0 && graceStart > 0 && time.UnixMilli(graceStart).Before(graceCutoff) {
			expiredVaults = append(expiredVaults, id)
			unlock()
			continue
		}
		pruned := false
		for objID, p := range m.Pending {
			if p.expired(now, v.grace) {
				delete(m.Pending, objID)
				pruned = true
				continue
			}
			live[p.ID] = struct{}{}
		}
		if pruned {
			if werr := v.writeMeta(id, m); werr != nil {
				slog.Warn("gc: prune pending failed", "vault", id, "err", werr)
			}
		}
		for _, ver := range v.keptVersions(id) {
			refs, rerr := v.refs(id, ver)
			if rerr != nil {
				unlock()
				return nil, nil, fmt.Errorf("vault %s manifest %d refs: %w", id, ver, rerr)
			}
			for _, r := range refs {
				live[r.ID] = struct{}{}
			}
		}
		unlock()
	}
	return live, expiredVaults, nil
}
