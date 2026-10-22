// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import "fmt"

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

	DefaultS3Region       = "us-east-1"
	DefaultPushBoxKeyPath = "./push.key"
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
		S3Region:         DefaultS3Region,
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
		PushBoxKeyPath:   DefaultPushBoxKeyPath,
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
