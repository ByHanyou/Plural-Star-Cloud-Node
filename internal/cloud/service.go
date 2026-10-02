// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
)

const (
	AgentName    = "plural-star-cloud-node"
	AgentVersion = "1.0.0"
)

type Service struct {
	cfg  config.CloudConfig
	root string

	vaults  *vaultStore
	objects *objectStore
	inbox   *Inbox
	fronts  *frontStore
	push    *pushRelay
	limiter *rateLimiter
	media   Backend
}

func New(ctx context.Context, cfg config.CloudConfig, configDir string) (*Service, error) {
	root := cfg.StoragePath
	if !filepath.IsAbs(root) {
		root = filepath.Join(configDir, root)
	}
	for _, sub := range []string{"vaults", "objects", "tmp", "inbox", "front", "push"} {
		if err := os.MkdirAll(filepath.Join(root, sub), 0o700); err != nil {
			return nil, fmt.Errorf("cloud: create %s: %w", sub, err)
		}
	}
	media, err := newBackend(cfg, filepath.Join(root, "objects"))
	if err != nil {
		return nil, err
	}
	s := &Service{cfg: cfg, root: root, media: media}
	s.limiter = newRateLimiter()
	s.objects = newObjectStore(root, s.media, cfg)
	s.vaults = newVaultStore(root, cfg, s.objects)
	s.inbox = newInbox(filepath.Join(root, "inbox"), cfg)
	s.inbox.full = s.atWatermark
	s.fronts = newFrontStore(filepath.Join(root, "front"))
	pushKeyPath := cfg.PushBoxKeyPath
	if pushKeyPath == "" {
		pushKeyPath = "./push.key"
	}
	if !filepath.IsAbs(pushKeyPath) {
		pushKeyPath = filepath.Join(configDir, pushKeyPath)
	}
	push, err := newPushRelay(ctx, filepath.Join(root, "push"), pushKeyPath, cfg)
	if err != nil {
		return nil, err
	}
	s.push = push
	go s.gcLoop(ctx)
	go s.limiter.sweepLoop(ctx)
	log.Printf("cloud: services ready at %s (media backend %s)", root, cfg.MediaBackend)
	return s, nil
}

func (s *Service) Inbox() *Inbox { return s.inbox }

func (s *Service) PushBoxPublicKey() string { return s.push.publicKeyB64() }

func AgentString(roles []string, pushBoxPub string) string {
	agent := AgentName + "/" + AgentVersion + " roles=" + strings.Join(roles, ",")
	if pushBoxPub != "" {
		agent += " pushbox=" + pushBoxPub
	}
	return agent
}

func ParseAgent(agent string) (roles []string, pushBoxPub string) {
	for _, field := range strings.Fields(agent) {
		if strings.HasPrefix(field, "roles=") {
			for _, r := range strings.Split(strings.TrimPrefix(field, "roles="), ",") {
				if r != "" {
					roles = append(roles, r)
				}
			}
		} else if strings.HasPrefix(field, "pushbox=") {
			pushBoxPub = strings.TrimPrefix(field, "pushbox=")
		}
	}
	if len(roles) == 0 {
		roles = []string{"relay"}
	}
	return roles, pushBoxPub
}

func (s *Service) HealthFields() map[string]any {
	used, free, total := diskUsage(s.root)
	out := map[string]any{
		"vaults":        s.vaults.count(),
		"used_bytes":    used,
		"free_bytes":    free,
		"total_bytes":   total,
		"watermark":     s.cfg.WatermarkPercent,
		"at_watermark":  s.atWatermark(),
		"inbox_packets": s.inbox.count(),
		"push_queue":    s.push.queueLen(),
		"push_relay":    s.cfg.PushForwardURL != "",
		"media_backend": s.cfg.MediaBackend,
	}
	if pub := s.push.publicKeyB64(); pub != "" {
		out["push_box_pub"] = pub
	}
	return out
}

func Pressure() map[string]any {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	out := map[string]any{
		"heap_bytes": ms.HeapAlloc,
		"sys_bytes":  ms.Sys,
		"goroutines": runtime.NumGoroutine(),
		"cpus":       runtime.NumCPU(),
	}
	if raw, err := os.ReadFile("/proc/loadavg"); err == nil {
		fields := strings.Fields(string(raw))
		if len(fields) >= 3 {
			if l1, e := strconv.ParseFloat(fields[0], 64); e == nil {
				out["load1"] = l1
			}
			if l5, e := strconv.ParseFloat(fields[1], 64); e == nil {
				out["load5"] = l5
			}
			if l15, e := strconv.ParseFloat(fields[2], 64); e == nil {
				out["load15"] = l15
			}
		}
	}
	if limit := os.Getenv("GOMEMLIMIT"); limit != "" {
		out["gomemlimit"] = limit
	}
	return out
}

func (s *Service) atWatermark() bool {
	used, _, total := diskUsage(s.root)
	if total == 0 {
		return false
	}
	return used*100 >= total*uint64(s.cfg.WatermarkPercent)
}

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
		return time.Time{}
	}
	ms, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

func writeGCState(path string, at time.Time) {
	_ = os.WriteFile(path, []byte(strconv.FormatInt(at.UnixMilli(), 10)), 0o600)
}
