// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"log"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
)

const gcObjectGrace = 24 * time.Hour

// runGC is the daily collection from section 7.5: objects no kept manifest
// references, vaults whose zero-device grace has elapsed, expired inbox
// packets, and abandoned upload fragments.
func (s *Service) runGC() {
	start := time.Now()
	graceCutoff := start.Add(-time.Duration(s.cfg.GraceDays) * 24 * time.Hour)
	live, expired := s.vaults.liveObjects(graceCutoff)
	for _, id := range expired {
		if err := s.vaults.Delete(id); err != nil {
			log.Printf("gc: delete vault %s: %v", id, err)
		}
	}
	if len(expired) > 0 {
		live, _ = s.vaults.liveObjects(graceCutoff)
	}
	removed, freed := s.collectObjects(s.objects.local, live, start)
	if s.media.Name() != config.MediaBackendLocal {
		r, f := s.collectObjects(s.media, live, start)
		removed += r
		freed += f
	}
	s.objects.sweepParts(gcObjectGrace)
	s.inbox.sweep()
	s.pruneLocks()
	log.Printf("gc: %d vault(s) expired, %d object(s) removed (%d bytes), %d live, %s",
		len(expired), removed, freed, len(live), time.Since(start).Round(time.Millisecond))
}

func (s *Service) collectObjects(b Backend, live map[string]struct{}, now time.Time) (int, int64) {
	var doomed []string
	var freed int64
	err := b.List(func(id string, size int64, modified time.Time) error {
		if _, ok := live[id]; ok {
			return nil
		}
		if now.Sub(modified) < gcObjectGrace {
			return nil
		}
		doomed = append(doomed, id)
		freed += size
		return nil
	})
	if err != nil {
		log.Printf("gc: list %s objects: %v", b.Name(), err)
		return 0, 0
	}
	removed := 0
	for _, id := range doomed {
		if err := b.Delete(id); err != nil {
			log.Printf("gc: delete %s object %s: %v", b.Name(), id, err)
			continue
		}
		removed++
	}
	return removed, freed
}

func (s *Service) pruneLocks() {
	s.vaults.mu.Lock()
	for k, m := range s.vaults.locks {
		if m.TryLock() {
			delete(s.vaults.locks, k)
			m.Unlock()
		}
	}
	s.vaults.mu.Unlock()
	s.objects.mu.Lock()
	for k, m := range s.objects.locks {
		if m.TryLock() {
			delete(s.objects.locks, k)
			m.Unlock()
		}
	}
	s.objects.mu.Unlock()
}
