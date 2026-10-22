// SPDX-License-Identifier: AGPL-3.0-or-later

package network

import (
	"fmt"
	"os"
	"slices"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
)

func ApplyInvite(cfg config.Config, inv Invite, pskPath string) (config.Config, error) {
	if pskPath == "" {
		pskPath = config.DefaultPSKPath
	}
	if len(inv.PSK) != pskLen {
		return cfg, fmt.Errorf("invite psk must be %d bytes", pskLen)
	}
	if err := os.WriteFile(pskPath, inv.PSK, 0o600); err != nil {
		return cfg, fmt.Errorf("write psk %q: %w", pskPath, err)
	}

	cfg.NetworkMode = config.ModePrivate
	cfg.PSKPath = pskPath
	if inv.Label != "" {
		cfg.NetworkID = inv.Label
	}
	peers := slices.Clone(cfg.BootstrapPeers)
	for _, a := range inv.Multiaddrs {
		s := a.String()
		if !slices.Contains(peers, s) {
			peers = append(peers, s)
		}
	}
	cfg.BootstrapPeers = peers
	return cfg, nil
}
