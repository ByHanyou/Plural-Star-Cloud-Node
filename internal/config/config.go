// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"fmt"
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

const (
	ModePublic       = "public"
	ModePrivate      = "private"
	ModeCustomPublic = "custom_public"
)

const DefaultAPIPort = 7523

type Config struct {
	KeypairPath string `yaml:"keypair_path" json:"keypair_path"`

	// RequireExistingIdentity makes a missing keypair file a fatal error
	// instead of generating a new one. The peer ID derived from node.key is
	// compiled into every shipped app as the bootstrap entry, so on the
	// production node a silently regenerated key is an outage that looks
	// like a normal start. Community nodes leave this false.
	RequireExistingIdentity bool `yaml:"require_existing_identity" json:"require_existing_identity"`

	NetworkMode string `yaml:"network_mode" json:"network_mode"`

	PSKPath string `yaml:"psk_path" json:"psk_path"`

	NetworkID string `yaml:"network_id" json:"network_id"`

	BootstrapPeers []string `yaml:"bootstrap_peers" json:"bootstrap_peers"`

	DirectoryURL string `yaml:"directory_url" json:"directory_url"`

	APIHost  string `yaml:"api_host" json:"api_host"`
	APIPort  int    `yaml:"api_port" json:"api_port"`
	APIToken string `yaml:"api_token" json:"api_token"`

	// DebugAddr turns on Go's pprof handlers on their OWN listener. Empty (the
	// default) means off. It must be a loopback address and is refused
	// otherwise: the main API binds 0.0.0.0 in production and pprof would hand
	// the internet full heap and goroutine dumps. Reach it over an SSH tunnel.
	// Exists because diagnosing the 2026-09 goroutine leak from signals alone
	// was impossible: SIGQUIT needs GOTRACEBACK=all and journald silently
	// rate-limits the dump away.
	DebugAddr string `yaml:"debug_addr" json:"debug_addr"`

	ListenAddrs   []string `yaml:"listen_addrs" json:"listen_addrs"`
	AnnounceAddrs []string `yaml:"announce_addrs" json:"announce_addrs"`

	RelayEnabled bool `yaml:"relay_enabled" json:"relay_enabled"`

	MaxPeers          int `yaml:"max_peers" json:"max_peers"`
	MaxAppConnections int `yaml:"max_app_connections" json:"max_app_connections"`

	Cloud CloudConfig `yaml:"cloud" json:"cloud"`
}

type CloudConfig struct {
	Enabled          bool   `yaml:"enabled" json:"enabled"`
	StoragePath      string `yaml:"storage_path" json:"storage_path"`
	MediaBackend     string `yaml:"media_backend" json:"media_backend"`
	RemoteBlobURL    string `yaml:"remote_blob_url" json:"remote_blob_url"`
	RemoteBlobSecret string `yaml:"remote_blob_secret" json:"remote_blob_secret"`
	BlobServeSecret  string `yaml:"blob_serve_secret" json:"blob_serve_secret"`
	S3Endpoint       string `yaml:"s3_endpoint" json:"s3_endpoint"`
	S3Region         string `yaml:"s3_region" json:"s3_region"`
	S3Bucket         string `yaml:"s3_bucket" json:"s3_bucket"`
	S3Prefix         string `yaml:"s3_prefix" json:"s3_prefix"`
	S3AccessKey      string `yaml:"s3_access_key" json:"s3_access_key"`
	S3SecretKey      string `yaml:"s3_secret_key" json:"s3_secret_key"`
	WatermarkPercent int    `yaml:"watermark_percent" json:"watermark_percent"`
	BaseQuotaMB      int    `yaml:"base_quota_mb" json:"base_quota_mb"`
	MediaQuotaMB     int    `yaml:"media_quota_mb" json:"media_quota_mb"`
	ObjectMaxMB      int    `yaml:"object_max_mb" json:"object_max_mb"`
	ManifestVersions int    `yaml:"manifest_versions" json:"manifest_versions"`
	InboxTTLDays     int    `yaml:"inbox_ttl_days" json:"inbox_ttl_days"`
	InboxQuotaMB     int    `yaml:"inbox_quota_mb" json:"inbox_quota_mb"`
	GraceDays        int    `yaml:"grace_days" json:"grace_days"`
	GCHourUTC        int    `yaml:"gc_hour_utc" json:"gc_hour_utc"`
	PushForwardURL   string `yaml:"push_forward_url" json:"push_forward_url"`
	PushForwardToken string `yaml:"push_forward_token" json:"push_forward_token"`
	PushBoxKeyPath   string `yaml:"push_box_key_path" json:"push_box_key_path"`
}

const (
	MediaBackendLocal  = "local"
	MediaBackendRemote = "remote"
	MediaBackendS3     = "s3"
)

func DefaultCloud() CloudConfig {
	return CloudConfig{
		Enabled:          false,
		StoragePath:      "./cloud",
		MediaBackend:     MediaBackendLocal,
		RemoteBlobURL:    "",
		RemoteBlobSecret: "",
		BlobServeSecret:  "",
		S3Endpoint:       "",
		S3Region:         "us-east-1",
		S3Bucket:         "",
		S3Prefix:         "",
		S3AccessKey:      "",
		S3SecretKey:      "",
		WatermarkPercent: 90,
		BaseQuotaMB:      200,
		MediaQuotaMB:     1024,
		ObjectMaxMB:      25,
		ManifestVersions: 30,
		InboxTTLDays:     30,
		InboxQuotaMB:     50,
		GraceDays:        30,
		GCHourUTC:        4,
		PushForwardURL:   "",
		PushForwardToken: "",
		PushBoxKeyPath:   "./push.key",
	}
}

func (c *CloudConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.StoragePath == "" {
		return fmt.Errorf("cloud.storage_path is required when cloud.enabled is true")
	}
	switch c.MediaBackend {
	case MediaBackendLocal:
	case MediaBackendRemote:
		if c.RemoteBlobURL == "" || c.RemoteBlobSecret == "" {
			return fmt.Errorf("cloud.media_backend=remote requires remote_blob_url and remote_blob_secret")
		}
	case MediaBackendS3:
		if c.S3Endpoint == "" || c.S3Bucket == "" || c.S3AccessKey == "" || c.S3SecretKey == "" {
			return fmt.Errorf("cloud.media_backend=s3 requires s3_endpoint, s3_bucket, s3_access_key and s3_secret_key")
		}
	default:
		return fmt.Errorf("invalid cloud.media_backend %q (want local, remote, or s3)", c.MediaBackend)
	}
	if c.WatermarkPercent < 50 || c.WatermarkPercent > 99 {
		return fmt.Errorf("cloud.watermark_percent %d out of range (50-99)", c.WatermarkPercent)
	}
	if c.BaseQuotaMB <= 0 || c.MediaQuotaMB <= 0 || c.ObjectMaxMB <= 0 {
		return fmt.Errorf("cloud quotas must be positive")
	}
	if c.ManifestVersions <= 0 {
		return fmt.Errorf("cloud.manifest_versions must be positive")
	}
	if c.InboxTTLDays <= 0 || c.InboxQuotaMB <= 0 {
		return fmt.Errorf("cloud.inbox_ttl_days and cloud.inbox_quota_mb must be positive")
	}
	if c.GraceDays <= 0 {
		return fmt.Errorf("cloud.grace_days must be positive")
	}
	if c.GCHourUTC < 0 || c.GCHourUTC > 23 {
		return fmt.Errorf("cloud.gc_hour_utc %d out of range (0-23)", c.GCHourUTC)
	}
	return nil
}

func Default() *Config {
	return &Config{
		KeypairPath:    "./node.key",
		NetworkMode:    ModePublic,
		PSKPath:        "./network.psk",
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
	}
}

func Load(path string) (cfg *Config, firstRun bool, err error) {
	data, readErr := os.ReadFile(path)
	if readErr != nil {
		if !os.IsNotExist(readErr) {
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

	// A node on the default public network exists to serve app users who have
	// no token and never will, so every endpoint an app touches is behind the
	// same auth gate. An api_token here therefore does not secure the node, it
	// shuts the whole network out, and because nothing reads the file until the
	// next start the damage stays invisible until a reboot. Strip it, say so,
	// and write the file back so it cannot come back a third time. Private and
	// custom_public networks are operator-run and still honour a token.
	if cfg.NetworkMode == ModePublic && cfg.APIToken != "" {
		cfg.APIToken = ""
		log.Printf("config: api_token ignored and removed from %s. A public node must stay open or it locks out every app on the default network.", path)
		if err := Save(cfg, path); err != nil {
			return nil, false, fmt.Errorf("clear api_token in %q: %w", path, err)
		}
	}

	if err := cfg.Validate(); err != nil {
		return nil, false, err
	}
	return cfg, firstRun, nil
}

func Save(cfg *Config, path string) error {
	out, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	return os.WriteFile(path, out, 0o600)
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
	default:
		return fmt.Errorf("invalid network_mode %q (want public, private, or custom_public)", c.NetworkMode)
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
	return c.Cloud.Validate()
}

func (c *Config) Roles() []string {
	roles := []string{"relay"}
	if c.Cloud.Enabled {
		roles = append(roles, "cloud")
	}
	return roles
}
