// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	ModePublic       = "public"
	ModePrivate      = "private"
	ModeCustomPublic = "custom_public"
)

const (
	DefaultAPIPort     = 7523
	DefaultKeypairPath = "./node.key"
	DefaultPSKPath     = "./network.psk"
)

const (
	LogLevelDebug = "debug"
	LogLevelInfo  = "info"
	LogLevelWarn  = "warn"
	LogLevelError = "error"
)

type Config struct {
	KeypairPath string `yaml:"keypair_path" json:"keypair_path"`

	RequireExistingIdentity bool `yaml:"require_existing_identity" json:"require_existing_identity"`

	NetworkMode string `yaml:"network_mode" json:"network_mode"`

	PSKPath string `yaml:"psk_path" json:"psk_path"`

	NetworkID string `yaml:"network_id" json:"network_id"`

	BootstrapPeers []string `yaml:"bootstrap_peers" json:"bootstrap_peers"`

	DirectoryURL string `yaml:"directory_url" json:"directory_url"`

	APIHost  string `yaml:"api_host" json:"api_host"`
	APIPort  int    `yaml:"api_port" json:"api_port"`
	APIToken string `yaml:"api_token" json:"api_token"`

	DebugAddr string `yaml:"debug_addr" json:"debug_addr"`

	ListenAddrs   []string `yaml:"listen_addrs" json:"listen_addrs"`
	AnnounceAddrs []string `yaml:"announce_addrs" json:"announce_addrs"`

	RelayEnabled bool `yaml:"relay_enabled" json:"relay_enabled"`

	MaxPeers          int `yaml:"max_peers" json:"max_peers"`
	MaxAppConnections int `yaml:"max_app_connections" json:"max_app_connections"`

	Cloud CloudConfig `yaml:"cloud" json:"cloud"`

	LogLevel string `yaml:"log_level" json:"log_level"`

	Limits Limits `yaml:"limits" json:"limits"`
}

func Default() *Config {
	return &Config{
		KeypairPath:    DefaultKeypairPath,
		NetworkMode:    ModePublic,
		PSKPath:        DefaultPSKPath,
		NetworkID:      "",
		BootstrapPeers: []string{},
		DirectoryURL:   "",
		APIHost:        "127.0.0.1",
		APIPort:        DefaultAPIPort,
		APIToken:       "",
		ListenAddrs: []string{
			"/ip4/0.0.0.0/tcp/4001",
			"/ip4/0.0.0.0/udp/4001/quic-v1",
		},
		AnnounceAddrs:     []string{},
		RelayEnabled:      true,
		MaxPeers:          200,
		MaxAppConnections: 5000,
		Cloud:             DefaultCloud(),
		LogLevel:          LogLevelInfo,
		Limits:            DefaultLimits(),
	}
}

func Load(path string) (cfg *Config, firstRun bool, err error) {
	data, readErr := os.ReadFile(path)
	if readErr != nil {
		if !errors.Is(readErr, os.ErrNotExist) {
			return nil, false, fmt.Errorf("read config %q: %w", path, readErr)
		}
		cfg = Default()
		if err := Save(cfg, path); err != nil {
			return nil, false, fmt.Errorf("write initial config %q: %w", path, err)
		}
		if err := cfg.Validate(); err != nil {
			return nil, false, err
		}
		return cfg, true, nil
	}

	cfg = Default()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, false, fmt.Errorf("parse config %q: %w", path, err)
	}

	if cfg.NetworkMode == ModePublic && cfg.APIToken != "" {
		cfg.APIToken = ""
		slog.Warn("config: api_token ignored and removed; a public node must stay open or it locks out every app on the default network", "path", path)
		if err := Save(cfg, path); err != nil {
			return nil, false, fmt.Errorf("clear api_token in %q: %w", path, err)
		}
	}

	if err := cfg.Validate(); err != nil {
		return nil, false, err
	}
	return cfg, false, nil
}

func Save(cfg *Config, path string) error {
	out, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, out, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (c *Config) Validate() error {
	switch c.NetworkMode {
	case ModePublic:
	case ModePrivate:
		if c.PSKPath == "" {
			return fmt.Errorf("network_mode=private requires psk_path")
		}
		if len(c.BootstrapPeers) == 0 {
			return fmt.Errorf("network_mode=private requires at least one bootstrap_peers entry")
		}
	case ModeCustomPublic:
		if c.NetworkID == "" {
			return fmt.Errorf("network_mode=custom_public requires network_id")
		}
		if len(c.BootstrapPeers) == 0 {
			return fmt.Errorf("network_mode=custom_public requires at least one bootstrap_peers entry")
		}
	default:
		return fmt.Errorf("invalid network_mode %q (want public, private, or custom_public)", c.NetworkMode)
	}

	if c.KeypairPath == "" {
		return fmt.Errorf("keypair_path is required")
	}
	if c.APIPort <= 0 || c.APIPort > 65535 {
		return fmt.Errorf("api_port %d out of range", c.APIPort)
	}
	if len(c.ListenAddrs) == 0 {
		return fmt.Errorf("at least one listen_addr is required")
	}
	if c.MaxPeers <= 0 {
		return fmt.Errorf("max_peers must be positive")
	}
	if c.MaxAppConnections <= 0 {
		return fmt.Errorf("max_app_connections must be positive")
	}
	if !ValidLogLevel(c.LogLevel) {
		return fmt.Errorf("invalid log_level %q (want debug, info, warn, or error)", c.LogLevel)
	}
	if err := c.Limits.Validate(); err != nil {
		return err
	}
	return c.Cloud.Validate()
}

func ValidLogLevel(level string) bool {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case LogLevelDebug, LogLevelInfo, LogLevelWarn, LogLevelError:
		return true
	}
	return false
}

func (c *Config) Roles() []string {
	roles := []string{"relay"}
	if c.Cloud.Enabled {
		roles = append(roles, "cloud")
	}
	return roles
}
