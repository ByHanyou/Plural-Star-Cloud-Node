// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Duration time.Duration

func (d Duration) Std() time.Duration { return time.Duration(d) }

func (d Duration) String() string {
	s := time.Duration(d).String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}

func (d Duration) MarshalYAML() (any, error) { return d.String(), nil }

func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		return fmt.Errorf("duration: want a string like \"90s\" or \"5m\", got %q", n.Value)
	}
	return d.parse(s)
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration: want a string like \"90s\" or \"5m\", got %s", string(b))
	}
	return d.parse(s)
}

func (d *Duration) parse(s string) error {
	v, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return fmt.Errorf("duration %q: %w", s, err)
	}
	*d = Duration(v)
	return nil
}

type Limits struct {
	ReconnectInterval         Duration `yaml:"reconnect_interval" json:"reconnect_interval"`
	ConnectionMonitorInterval Duration `yaml:"connection_monitor_interval" json:"connection_monitor_interval"`
	DialConcurrency           int      `yaml:"dial_concurrency" json:"dial_concurrency"`
	DialTimeout               Duration `yaml:"dial_timeout" json:"dial_timeout"`
	ConnmgrGracePeriod        Duration `yaml:"connmgr_grace_period" json:"connmgr_grace_period"`
	PingTimeout               Duration `yaml:"ping_timeout" json:"ping_timeout"`
	DiscoveryInterval         Duration `yaml:"discovery_interval" json:"discovery_interval"`
	DiscoveryDialTimeout      Duration `yaml:"discovery_dial_timeout" json:"discovery_dial_timeout"`
	DirectoryFetchTimeout     Duration `yaml:"directory_fetch_timeout" json:"directory_fetch_timeout"`
	PeerMemoryMaxPeers        int      `yaml:"peer_memory_max_peers" json:"peer_memory_max_peers"`
	PeerMemoryMaxAddrs        int      `yaml:"peer_memory_max_addrs" json:"peer_memory_max_addrs"`
	PeerMemoryTTL             Duration `yaml:"peer_memory_ttl" json:"peer_memory_ttl"`

	MaxPacketBytes          int      `yaml:"max_packet_bytes" json:"max_packet_bytes"`
	RelayStreamTimeout      Duration `yaml:"relay_stream_timeout" json:"relay_stream_timeout"`
	PresenceTTL             Duration `yaml:"presence_ttl" json:"presence_ttl"`
	MaxPresenceTTL          Duration `yaml:"max_presence_ttl" json:"max_presence_ttl"`
	PresenceRefreshInterval Duration `yaml:"presence_refresh_interval" json:"presence_refresh_interval"`
	DedupCacheTTL           Duration `yaml:"dedup_cache_ttl" json:"dedup_cache_ttl"`
	DedupEvictInterval      Duration `yaml:"dedup_evict_interval" json:"dedup_evict_interval"`
	RoutingPruneInterval    Duration `yaml:"routing_prune_interval" json:"routing_prune_interval"`
	QueueEntryTTL           Duration `yaml:"queue_entry_ttl" json:"queue_entry_ttl"`
	QueueMaxSenders         int      `yaml:"queue_max_senders" json:"queue_max_senders"`
	QueueMaxRecipients      int      `yaml:"queue_max_recipients" json:"queue_max_recipients"`
	QueueMaxBytes           int      `yaml:"queue_max_bytes" json:"queue_max_bytes"`
	QueueSweepInterval      Duration `yaml:"queue_sweep_interval" json:"queue_sweep_interval"`

	RendezvousMaxEntries         int      `yaml:"rendezvous_max_entries" json:"rendezvous_max_entries"`
	RendezvousMaxNamespaceLen    int      `yaml:"rendezvous_max_namespace_len" json:"rendezvous_max_namespace_len"`
	RendezvousMaxRecordBytes     int      `yaml:"rendezvous_max_record_bytes" json:"rendezvous_max_record_bytes"`
	RendezvousMaxTTL             Duration `yaml:"rendezvous_max_ttl" json:"rendezvous_max_ttl"`
	RendezvousReannounceInterval Duration `yaml:"rendezvous_reannounce_interval" json:"rendezvous_reannounce_interval"`
	WSWriteWait                  Duration `yaml:"ws_write_wait" json:"ws_write_wait"`
	WSPongWait                   Duration `yaml:"ws_pong_wait" json:"ws_pong_wait"`
	WSPingPeriod                 Duration `yaml:"ws_ping_period" json:"ws_ping_period"`
	WSSendBuffer                 int      `yaml:"ws_send_buffer" json:"ws_send_buffer"`
	WSMaxMessageBytes            int      `yaml:"ws_max_message_bytes" json:"ws_max_message_bytes"`
	HTTPReadHeaderTimeout        Duration `yaml:"http_read_header_timeout" json:"http_read_header_timeout"`
	HTTPIdleTimeout              Duration `yaml:"http_idle_timeout" json:"http_idle_timeout"`
	ShutdownTimeout              Duration `yaml:"shutdown_timeout" json:"shutdown_timeout"`
	MaxSendBodyBytes             int      `yaml:"max_send_body_bytes" json:"max_send_body_bytes"`
	MaxInviteBodyBytes           int      `yaml:"max_invite_body_bytes" json:"max_invite_body_bytes"`
	MaxRendezvousBodyBytes       int      `yaml:"max_rendezvous_body_bytes" json:"max_rendezvous_body_bytes"`

	AuthTimestampSkew    Duration `yaml:"auth_timestamp_skew" json:"auth_timestamp_skew"`
	MaxJSONBodyBytes     int      `yaml:"max_json_body_bytes" json:"max_json_body_bytes"`
	MaxManifestBodyBytes int      `yaml:"max_manifest_body_bytes" json:"max_manifest_body_bytes"`
	RateLimitPerAddress  int      `yaml:"rate_limit_per_address_per_minute" json:"rate_limit_per_address_per_minute"`
	RateLimitPerVault    int      `yaml:"rate_limit_per_vault_per_hour" json:"rate_limit_per_vault_per_hour"`
	GCObjectGrace        Duration `yaml:"gc_object_grace" json:"gc_object_grace"`
	FrontMaxAge          Duration `yaml:"front_max_age" json:"front_max_age"`
	PushRegisterTTL      Duration `yaml:"push_register_ttl" json:"push_register_ttl"`
	PushEventTTL         Duration `yaml:"push_event_ttl" json:"push_event_ttl"`
	PushMaxBodyBytes     int      `yaml:"push_max_body_bytes" json:"push_max_body_bytes"`
	PushMaxBackoff       Duration `yaml:"push_max_backoff" json:"push_max_backoff"`
	PushHTTPTimeout      Duration `yaml:"push_http_timeout" json:"push_http_timeout"`
	BlobHTTPTimeout      Duration `yaml:"blob_http_timeout" json:"blob_http_timeout"`
}

func DefaultLimits() Limits {
	return Limits{
		ReconnectInterval:         Duration(90 * time.Second),
		ConnectionMonitorInterval: Duration(5 * time.Minute),
		DialConcurrency:           8,
		DialTimeout:               Duration(15 * time.Second),
		ConnmgrGracePeriod:        Duration(time.Minute),
		PingTimeout:               Duration(10 * time.Second),
		DiscoveryInterval:         Duration(60 * time.Second),
		DiscoveryDialTimeout:      Duration(20 * time.Second),
		DirectoryFetchTimeout:     Duration(15 * time.Second),
		PeerMemoryMaxPeers:        64,
		PeerMemoryMaxAddrs:        8,
		PeerMemoryTTL:             Duration(30 * 24 * time.Hour),

		MaxPacketBytes:          2 << 20,
		RelayStreamTimeout:      Duration(15 * time.Second),
		PresenceTTL:             Duration(90 * time.Second),
		MaxPresenceTTL:          Duration(10 * time.Minute),
		PresenceRefreshInterval: Duration(45 * time.Second),
		DedupCacheTTL:           Duration(10 * time.Second),
		DedupEvictInterval:      Duration(5 * time.Second),
		RoutingPruneInterval:    Duration(30 * time.Second),
		QueueEntryTTL:           Duration(7 * 24 * time.Hour),
		QueueMaxSenders:         500,
		QueueMaxRecipients:      10000,
		QueueMaxBytes:           64 << 20,
		QueueSweepInterval:      Duration(30 * time.Minute),

		RendezvousMaxEntries:         20000,
		RendezvousMaxNamespaceLen:    128,
		RendezvousMaxRecordBytes:     8 * 1024,
		RendezvousMaxTTL:             Duration(time.Hour),
		RendezvousReannounceInterval: Duration(5 * time.Minute),
		WSWriteWait:                  Duration(10 * time.Second),
		WSPongWait:                   Duration(60 * time.Second),
		WSPingPeriod:                 Duration(50 * time.Second),
		WSSendBuffer:                 64,
		WSMaxMessageBytes:            64 << 10,
		HTTPReadHeaderTimeout:        Duration(15 * time.Second),
		HTTPIdleTimeout:              Duration(2 * time.Minute),
		ShutdownTimeout:              Duration(5 * time.Second),
		MaxSendBodyBytes:             1 << 20,
		MaxInviteBodyBytes:           64 << 10,
		MaxRendezvousBodyBytes:       64 << 10,

		AuthTimestampSkew:    Duration(10 * time.Minute),
		MaxJSONBodyBytes:     64 << 10,
		MaxManifestBodyBytes: 32 << 20,
		RateLimitPerAddress:  10,
		RateLimitPerVault:    20,
		GCObjectGrace:        Duration(24 * time.Hour),
		FrontMaxAge:          Duration(30 * 24 * time.Hour),
		PushRegisterTTL:      Duration(7 * 24 * time.Hour),
		PushEventTTL:         Duration(12 * time.Hour),
		PushMaxBodyBytes:     16 << 10,
		PushMaxBackoff:       Duration(5 * time.Minute),
		PushHTTPTimeout:      Duration(30 * time.Second),
		BlobHTTPTimeout:      Duration(10 * time.Minute),
	}
}

func (l *Limits) Validate() error {
	durations := []struct {
		name string
		v    Duration
	}{
		{"reconnect_interval", l.ReconnectInterval},
		{"connection_monitor_interval", l.ConnectionMonitorInterval},
		{"dial_timeout", l.DialTimeout},
		{"connmgr_grace_period", l.ConnmgrGracePeriod},
		{"ping_timeout", l.PingTimeout},
		{"discovery_interval", l.DiscoveryInterval},
		{"discovery_dial_timeout", l.DiscoveryDialTimeout},
		{"directory_fetch_timeout", l.DirectoryFetchTimeout},
		{"peer_memory_ttl", l.PeerMemoryTTL},
		{"presence_ttl", l.PresenceTTL},
		{"max_presence_ttl", l.MaxPresenceTTL},
		{"presence_refresh_interval", l.PresenceRefreshInterval},
		{"dedup_cache_ttl", l.DedupCacheTTL},
		{"dedup_evict_interval", l.DedupEvictInterval},
		{"routing_prune_interval", l.RoutingPruneInterval},
		{"relay_stream_timeout", l.RelayStreamTimeout},
		{"queue_entry_ttl", l.QueueEntryTTL},
		{"queue_sweep_interval", l.QueueSweepInterval},
		{"rendezvous_max_ttl", l.RendezvousMaxTTL},
		{"rendezvous_reannounce_interval", l.RendezvousReannounceInterval},
		{"ws_write_wait", l.WSWriteWait},
		{"ws_pong_wait", l.WSPongWait},
		{"ws_ping_period", l.WSPingPeriod},
		{"http_read_header_timeout", l.HTTPReadHeaderTimeout},
		{"http_idle_timeout", l.HTTPIdleTimeout},
		{"shutdown_timeout", l.ShutdownTimeout},
		{"auth_timestamp_skew", l.AuthTimestampSkew},
		{"gc_object_grace", l.GCObjectGrace},
		{"front_max_age", l.FrontMaxAge},
		{"push_register_ttl", l.PushRegisterTTL},
		{"push_event_ttl", l.PushEventTTL},
		{"push_max_backoff", l.PushMaxBackoff},
		{"push_http_timeout", l.PushHTTPTimeout},
		{"blob_http_timeout", l.BlobHTTPTimeout},
	}
	for _, d := range durations {
		if d.v <= 0 {
			return fmt.Errorf("limits.%s must be a positive duration", d.name)
		}
	}
	counts := []struct {
		name string
		v    int
	}{
		{"dial_concurrency", l.DialConcurrency},
		{"peer_memory_max_peers", l.PeerMemoryMaxPeers},
		{"peer_memory_max_addrs", l.PeerMemoryMaxAddrs},
		{"max_packet_bytes", l.MaxPacketBytes},
		{"queue_max_senders", l.QueueMaxSenders},
		{"queue_max_recipients", l.QueueMaxRecipients},
		{"queue_max_bytes", l.QueueMaxBytes},
		{"rendezvous_max_entries", l.RendezvousMaxEntries},
		{"rendezvous_max_namespace_len", l.RendezvousMaxNamespaceLen},
		{"rendezvous_max_record_bytes", l.RendezvousMaxRecordBytes},
		{"ws_send_buffer", l.WSSendBuffer},
		{"ws_max_message_bytes", l.WSMaxMessageBytes},
		{"max_send_body_bytes", l.MaxSendBodyBytes},
		{"max_invite_body_bytes", l.MaxInviteBodyBytes},
		{"max_rendezvous_body_bytes", l.MaxRendezvousBodyBytes},
		{"max_json_body_bytes", l.MaxJSONBodyBytes},
		{"max_manifest_body_bytes", l.MaxManifestBodyBytes},
		{"rate_limit_per_address_per_minute", l.RateLimitPerAddress},
		{"rate_limit_per_vault_per_hour", l.RateLimitPerVault},
		{"push_max_body_bytes", l.PushMaxBodyBytes},
	}
	for _, c := range counts {
		if c.v <= 0 {
			return fmt.Errorf("limits.%s must be positive", c.name)
		}
	}
	if l.WSPingPeriod >= l.WSPongWait {
		return fmt.Errorf("limits.ws_ping_period must be shorter than limits.ws_pong_wait")
	}
	if l.PresenceTTL > l.MaxPresenceTTL {
		return fmt.Errorf("limits.presence_ttl must not exceed limits.max_presence_ttl")
	}
	if l.PresenceRefreshInterval >= l.PresenceTTL {
		return fmt.Errorf("limits.presence_refresh_interval must be shorter than limits.presence_ttl")
	}
	if l.RendezvousReannounceInterval >= l.RendezvousMaxTTL {
		return fmt.Errorf("limits.rendezvous_reannounce_interval must be shorter than limits.rendezvous_max_ttl")
	}
	return nil
}
