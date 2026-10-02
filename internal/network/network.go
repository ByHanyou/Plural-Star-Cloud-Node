// SPDX-License-Identifier: AGPL-3.0-or-later

package network

import (
	"fmt"
	"os"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
)

const GlobalScope = "global"

var DefaultBootstrapPeers = []string{
	"/dns4/pluralstar.dedyn.io/tcp/4001/p2p/12D3KooWL4A25M2sWt1HoFY3r8hWn2idbc4yVdsdBqpFitRTNyfz",
}

func Scope(cfg *config.Config) string {
	if cfg.NetworkMode == config.ModeCustomPublic {
		return cfg.NetworkID
	}
	return GlobalScope
}

func DHTPrefix(cfg *config.Config) string {
	return "/plural-star/" + Scope(cfg)
}

func GossipPrefix(cfg *config.Config) string {
	return "plural-star:" + Scope(cfg) + ":"
}

func BootstrapPeers(cfg *config.Config) []string {
	if len(cfg.BootstrapPeers) > 0 {
		return cfg.BootstrapPeers
	}
	if cfg.NetworkMode == config.ModePublic {
		return DefaultBootstrapPeers
	}
	return nil
}

func LoadPSK(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read psk %q: %w", path, err)
	}
	if len(data) != 32 {
		return nil, fmt.Errorf("psk %q must be exactly 32 bytes, got %d", path, len(data))
	}
	return data, nil
}
