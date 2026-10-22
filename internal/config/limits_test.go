// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestDefaultLimitsValidate(t *testing.T) {
	l := DefaultLimits()
	if err := l.Validate(); err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}
	if l.PresenceTTL.Std() != 90*time.Second || l.MaxPacketBytes != 2<<20 || l.QueueMaxBytes != 64<<20 {
		t.Fatalf("defaults changed: %+v", l)
	}
}

func TestLimitsPartialOverrideKeepsOtherDefaults(t *testing.T) {
	cfg := Default()
	src := "limits:\n  presence_ttl: \"2m\"\n  queue_max_senders: 7\n"
	if err := yaml.Unmarshal([]byte(src), cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if cfg.Limits.PresenceTTL.Std() != 2*time.Minute {
		t.Fatalf("presence_ttl not applied: %v", cfg.Limits.PresenceTTL)
	}
	if cfg.Limits.QueueMaxSenders != 7 {
		t.Fatalf("queue_max_senders not applied: %d", cfg.Limits.QueueMaxSenders)
	}
	if cfg.Limits.QueueEntryTTL != DefaultLimits().QueueEntryTTL || cfg.Limits.WSPongWait != DefaultLimits().WSPongWait {
		t.Fatalf("untouched limits lost their defaults: %+v", cfg.Limits)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestLimitsRejectBadValues(t *testing.T) {
	cases := map[string]func(l *Limits){
		"zero duration":                        func(l *Limits) { l.DialTimeout = 0 },
		"negative count":                       func(l *Limits) { l.QueueMaxRecipients = -1 },
		"ping not before pong":                 func(l *Limits) { l.WSPingPeriod = l.WSPongWait },
		"presence over max":                    func(l *Limits) { l.PresenceTTL = l.MaxPresenceTTL + 1 },
		"refresh not before presence ttl":      func(l *Limits) { l.PresenceRefreshInterval = l.PresenceTTL },
		"reannounce not before rendezvous ttl": func(l *Limits) { l.RendezvousReannounceInterval = l.RendezvousMaxTTL },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			l := DefaultLimits()
			mutate(&l)
			if err := l.Validate(); err == nil {
				t.Fatal("expected a validation error")
			}
		})
	}
}

func TestDurationRequiresUnit(t *testing.T) {
	var l Limits
	err := yaml.Unmarshal([]byte("presence_ttl: 90\n"), &l)
	if err == nil || !strings.Contains(err.Error(), "duration") {
		t.Fatalf("a bare number must be rejected, got %v", err)
	}
}

func TestDurationJSONRoundTrip(t *testing.T) {
	in := Limits{DialTimeout: Duration(15 * time.Second)}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"dial_timeout":"15s"`) {
		t.Fatalf("json form: %s", b)
	}
	var out Limits
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.DialTimeout != in.DialTimeout {
		t.Fatalf("round trip: %v", out.DialTimeout)
	}
}

func TestLogLevelValidation(t *testing.T) {
	cfg := Default()
	for _, lvl := range []string{"debug", "info", "warn", "error", "WARN"} {
		cfg.LogLevel = lvl
		if err := cfg.Validate(); err != nil {
			t.Fatalf("%q should be valid: %v", lvl, err)
		}
	}
	cfg.LogLevel = "verbose"
	if err := cfg.Validate(); err == nil {
		t.Fatal("unknown log_level must fail validation")
	}
}

func TestSaveLoadKeepsLimitsAndLogLevel(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	cfg := Default()
	cfg.LogLevel = LogLevelDebug
	cfg.Limits.ReconnectInterval = Duration(45 * time.Second)
	if err := Save(cfg, path); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "reconnect_interval: 45s") || !strings.Contains(string(raw), "log_level: debug") {
		t.Fatalf("saved form:\n%s", raw)
	}
	loaded, _, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.LogLevel != LogLevelDebug || loaded.Limits.ReconnectInterval.Std() != 45*time.Second {
		t.Fatalf("loaded: level=%q reconnect=%v", loaded.LogLevel, loaded.Limits.ReconnectInterval)
	}
}

func TestDurationString(t *testing.T) {
	cases := map[Duration]string{
		Duration(90 * time.Second):           "1m30s",
		Duration(5 * time.Minute):            "5m",
		Duration(time.Hour):                  "1h",
		Duration(30 * 24 * time.Hour):        "720h",
		Duration(90 * time.Minute):           "1h30m",
		Duration(time.Hour + 30*time.Second): "1h0m30s",
		0:                                    "0s",
	}
	for d, want := range cases {
		if got := d.String(); got != want {
			t.Errorf("%d: got %q, want %q", int64(d), got, want)
		}
	}
}
