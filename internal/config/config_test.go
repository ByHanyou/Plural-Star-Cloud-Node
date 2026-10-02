// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"path/filepath"
	"testing"
)

func TestValidateNetworkModes(t *testing.T) {
	base := func() *Config {
		c := Default()
		c.APIToken = "x"
		return c
	}

	t.Run("public ok", func(t *testing.T) {
		if err := base().Validate(); err != nil {
			t.Fatalf("public should be valid: %v", err)
		}
	})

	t.Run("private requires psk and bootstrap", func(t *testing.T) {
		c := base()
		c.NetworkMode = ModePrivate
		c.PSKPath = ""
		if err := c.Validate(); err == nil {
			t.Fatal("private without psk_path should fail")
		}
		c.PSKPath = "./network.psk"
		c.BootstrapPeers = nil
		if err := c.Validate(); err == nil {
			t.Fatal("private without bootstrap_peers should fail")
		}
		c.BootstrapPeers = []string{"/ip4/1.2.3.4/tcp/4001/p2p/12D3KooWGFEV2PobB8q33b9MW5sCeKtpfTdWHyPmoV6sAYqZHcCU"}
		if err := c.Validate(); err != nil {
			t.Fatalf("private with psk+bootstrap should be valid: %v", err)
		}
	})

	t.Run("custom_public requires network_id", func(t *testing.T) {
		c := base()
		c.NetworkMode = ModeCustomPublic
		if err := c.Validate(); err == nil {
			t.Fatal("custom_public without network_id should fail")
		}
		c.NetworkID = "my-net"
		if err := c.Validate(); err != nil {
			t.Fatalf("custom_public with network_id should be valid: %v", err)
		}
	})

	t.Run("rejects bad fields", func(t *testing.T) {
		c := base()
		c.NetworkMode = "bogus"
		if err := c.Validate(); err == nil {
			t.Fatal("invalid network_mode should fail")
		}
		c = base()
		c.APIPort = 0
		if err := c.Validate(); err == nil {
			t.Fatal("api_port 0 should fail")
		}
		c = base()
		c.APIToken = ""
		if err := c.Validate(); err != nil {
			t.Fatalf("empty api_token is the required state for a public node: %v", err)
		}
		c = base()
		c.ListenAddrs = nil
		if err := c.Validate(); err == nil {
			t.Fatal("no listen_addrs should fail")
		}
	})
}

func TestLoadFirstRunWritesFileAndLeavesAuthOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg, firstRun, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !firstRun {
		t.Fatal("expected firstRun=true on a fresh path")
	}
	// A default node is public, and a public node must stay open: app users on
	// the default network have no token, and every endpoint is behind auth.
	if cfg.APIToken != "" {
		t.Fatalf("first run must leave api_token empty, got %q", cfg.APIToken)
	}

	cfg2, firstRun2, err := Load(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if firstRun2 {
		t.Fatal("expected firstRun=false on reload")
	}
	if cfg2.APIToken != "" {
		t.Fatalf("api_token must stay empty across loads, got %q", cfg2.APIToken)
	}
}

// A token that finds its way into a public node's config is stripped on load
// AND rewritten to disk, so it cannot survive to lock the network out again on
// the next boot. This is the 2026-07-31 outage, made unrepeatable.
func TestLoadStripsAPITokenFromPublicConfigAndPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	seed := Default()
	seed.APIToken = "9c4c5d67ce608408114975b1d4cc020d8e874eba91ae92f227492df7be312bee"
	if err := Save(seed, path); err != nil {
		t.Fatalf("seed: %v", err)
	}

	cfg, _, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.APIToken != "" {
		t.Fatalf("public api_token should have been stripped, got %q", cfg.APIToken)
	}

	reread, _, err := Load(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if reread.APIToken != "" {
		t.Fatal("stripped api_token was not written back to disk, so it would return on the next boot")
	}
}

// Operator-run networks are not the default network, so a token there is a
// deliberate choice and must be left alone.
func TestLoadKeepsAPITokenOnPrivateNetwork(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	seed := Default()
	seed.NetworkMode = ModePrivate
	seed.BootstrapPeers = []string{"/ip4/1.2.3.4/tcp/4001/p2p/12D3KooWGFEV2PobB8q33b9MW5sCeKtpfTdWHyPmoV6sAYqZHcCU"}
	seed.APIToken = "keep-me"
	if err := Save(seed, path); err != nil {
		t.Fatalf("seed: %v", err)
	}

	cfg, _, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.APIToken != "keep-me" {
		t.Fatalf("private network token should survive, got %q", cfg.APIToken)
	}
}
