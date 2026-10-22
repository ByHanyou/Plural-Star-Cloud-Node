// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
)

type Service struct {
	cfg  config.CloudConfig
	lim  config.Limits
	root string

	vaults  *vaultStore
	objects *objectStore
	inbox   *Inbox
	fronts  *frontStore
	push    *pushRelay
	limiter *rateLimiter
	media   Backend
}

func New(ctx context.Context, cfg config.CloudConfig, lim config.Limits, configDir string) (*Service, error) {
	root := cfg.StoragePath
	if !filepath.IsAbs(root) {
		root = filepath.Join(configDir, root)
	}
	for _, sub := range []string{"vaults", "objects", "tmp", "inbox", "front", "push"} {
		if err := os.MkdirAll(filepath.Join(root, sub), 0o700); err != nil {
			return nil, fmt.Errorf("cloud: create %s: %w", sub, err)
		}
	}
	media, err := newBackend(cfg, lim, filepath.Join(root, "objects"))
	if err != nil {
		return nil, err
	}
	s := &Service{cfg: cfg, lim: lim, root: root, media: media}
	s.limiter = newRateLimiter(lim.RateLimitPerAddress, lim.RateLimitPerVault)
	s.objects = newObjectStore(root, s.media, cfg)
	s.vaults = newVaultStore(root, cfg, s.objects, lim.GCObjectGrace.Std())
	s.inbox = newInbox(filepath.Join(root, "inbox"), cfg)
	s.inbox.full = s.atWatermark
	s.fronts = newFrontStore(filepath.Join(root, "front"))
	pushKeyPath := cfg.PushBoxKeyPath
	if pushKeyPath == "" {
		pushKeyPath = config.DefaultPushBoxKeyPath
	}
	if !filepath.IsAbs(pushKeyPath) {
		pushKeyPath = filepath.Join(configDir, pushKeyPath)
	}
	push, err := newPushRelay(ctx, filepath.Join(root, "push"), pushKeyPath, cfg, lim)
	if err != nil {
		return nil, err
	}
	s.push = push
	go s.gcLoop(ctx)
	go s.limiter.sweepLoop(ctx)
	slog.Info("cloud: services ready", "root", root, "media_backend", cfg.MediaBackend)
	return s, nil
}

func (s *Service) Inbox() *Inbox { return s.inbox }

func (s *Service) PushBoxPublicKey() string { return s.push.publicKeyB64() }

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

func (s *Service) atWatermark() bool {
	used, _, total := diskUsage(s.root)
	if total == 0 {
		return false
	}
	return used*100 >= total*uint64(s.cfg.WatermarkPercent)
}
