// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
)

type Middleware func(http.HandlerFunc) http.HandlerFunc

func (s *Service) Register(mux *http.ServeMux, authed Middleware) {
	mux.HandleFunc("POST /cloud/vault/lookup", authed(s.handleVaultLookup))
	mux.HandleFunc("POST /cloud/vault/create", authed(s.handleVaultCreate))
	mux.HandleFunc("GET /cloud/vault/info", authed(s.handleVaultInfo))
	mux.HandleFunc("GET /cloud/vault/manifest", authed(s.handleManifestGet))
	mux.HandleFunc("PUT /cloud/vault/manifest", authed(s.handleManifestPut))
	mux.HandleFunc("HEAD /cloud/vault/objects/{id}", authed(s.handleObjectHead))
	mux.HandleFunc("GET /cloud/vault/objects/{id}", authed(s.handleObjectGet))
	mux.HandleFunc("PUT /cloud/vault/objects/{id}", authed(s.handleObjectPut))
	mux.HandleFunc("POST /cloud/vault/devices", authed(s.handleDeviceLink))
	mux.HandleFunc("DELETE /cloud/vault/devices/{sub_id}", authed(s.handleDeviceUnlink))
	mux.HandleFunc("POST /cloud/vault/devices/{sub_id}/key", authed(s.handleDeviceKey))
	mux.HandleFunc("POST /cloud/vault/rewrap", authed(s.handleRewrap))
	mux.HandleFunc("GET /cloud/inbox", authed(s.handleInboxGet))
	mux.HandleFunc("PUT /cloud/front/{peer_id}", authed(s.handleFrontPut))
	mux.HandleFunc("GET /cloud/front/{peer_id}", authed(s.handleFrontGet))
	mux.HandleFunc("POST /cloud/push/register", authed(s.handlePushRegister))
	mux.HandleFunc("POST /cloud/push/event", authed(s.handlePushEvent))
	if s.cfg.BlobServeSecret != "" {
		mux.HandleFunc("GET /cloud/blob", s.blobAuthed(s.handleBlobList))
		mux.HandleFunc("PUT /cloud/blob/{id}", s.blobAuthed(s.handleBlobPut))
		mux.HandleFunc("GET /cloud/blob/{id}", s.blobAuthed(s.handleBlobGet))
		mux.HandleFunc("HEAD /cloud/blob/{id}", s.blobAuthed(s.handleBlobHead))
		mux.HandleFunc("DELETE /cloud/blob/{id}", s.blobAuthed(s.handleBlobDelete))
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeErrorCode(w, status, CodeForStatus(status), msg)
}

func writeErrorCode(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]string{"error": msg, "code": code})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, limit int64, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	err := json.NewDecoder(r.Body).Decode(v)
	if err == nil {
		return true
	}
	writeBodyError(w, err, "invalid JSON body")
	return false
}

func writeBodyError(w http.ResponseWriter, err error, msg string) {
	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}
	writeError(w, http.StatusBadRequest, msg)
}

type vaultCreds struct {
	LookupID   string `json:"lookup_id"`
	AuthSecret string `json:"auth_secret"`
}

func (s *Service) creds(r *http.Request, body *vaultCreds) (vaultCreds, bool) {
	c := vaultCreds{
		LookupID:   normalHex(r.Header.Get(headerVaultID)),
		AuthSecret: normalHex(r.Header.Get(headerVaultAuth)),
	}
	if body != nil {
		if c.LookupID == "" {
			c.LookupID = normalHex(body.LookupID)
		}
		if c.AuthSecret == "" {
			c.AuthSecret = normalHex(body.AuthSecret)
		}
	}
	if !validHex32(c.LookupID) || !validHex32(c.AuthSecret) {
		return c, false
	}
	return c, true
}

func (s *Service) limited(w http.ResponseWriter, r *http.Request, lookupID string) bool {
	ok, retry := s.limiter.check(clientIP(r), lookupID)
	if ok {
		return false
	}
	secs := int(retry.Seconds()) + 1
	w.Header().Set("Retry-After", strconv.Itoa(secs))
	writeJSON(w, http.StatusTooManyRequests, map[string]any{
		"error":               "rate limited",
		"code":                CodeRateLimited,
		"retry_after_seconds": secs,
	})
	return true
}

func (s *Service) watermarked(w http.ResponseWriter) bool {
	if !s.atWatermark() {
		return false
	}
	writeJSON(w, http.StatusInsufficientStorage, map[string]any{
		"error":     "disk at watermark",
		"code":      CodeDiskAtWatermark,
		"watermark": s.cfg.WatermarkPercent,
	})
	return true
}

func (s *Service) vaultError(w http.ResponseWriter, r *http.Request, lookupID string, err error) {
	var verr *versionError
	var merr *missingError
	switch {
	case errors.Is(err, ErrVaultMissing):
		s.limiter.record(clientIP(r), lookupID)
		writeErrorCode(w, http.StatusNotFound, CodeVaultMissing, "no vault")
	case errors.Is(err, ErrBadAuth):
		s.limiter.record(clientIP(r), lookupID)
		writeErrorCode(w, http.StatusUnauthorized, CodeBadAuth, "bad auth secret")
	case errors.Is(err, ErrDeviceKeyRequired):
		writeErrorCode(w, http.StatusUnauthorized, CodeDeviceKeyRequired, "device key required")
	case errors.Is(err, ErrDeviceUnlinked):
		writeErrorCode(w, http.StatusUnauthorized, CodeDeviceUnlinked, "device unlinked")
	case errors.Is(err, ErrDeviceKeyIssued):
		writeErrorCode(w, http.StatusConflict, CodeDeviceKeyIssued, "device key already issued")
	case errors.Is(err, ErrVaultExists):
		writeErrorCode(w, http.StatusConflict, CodeVaultExists, "vault already exists")
	case errors.As(err, &verr):
		writeJSON(w, http.StatusConflict, map[string]any{"error": "manifest version conflict", "code": CodeManifestVersionConflict, "version": verr.Current})
	case errors.As(err, &merr):
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "manifest references missing objects", "code": CodeManifestMissingObjects, "missing": merr.IDs})
	case errors.Is(err, ErrQuota):
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{
			"error":             "vault quota exceeded",
			"code":              CodeQuotaExceeded,
			"base_quota_bytes":  s.vaults.quota(TierBase),
			"media_quota_bytes": s.vaults.quota(TierMedia),
		})
	case errors.Is(err, ErrTooLarge):
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{
			"error":     "object too large",
			"code":      CodeObjectTooLarge,
			"max_bytes": s.objects.maxBytes(),
		})
	case errors.Is(err, ErrHashMismatch):
		writeErrorCode(w, http.StatusBadRequest, CodeHashMismatch, "object id does not match ciphertext hash")
	case errors.Is(err, ErrNotFound):
		writeErrorCode(w, http.StatusNotFound, CodeNotFound, "not found")
	default:
		writeErrorCode(w, http.StatusInternalServerError, CodeInternal, err.Error())
	}
}

func (s *Service) deviceRefused(w http.ResponseWriter, r *http.Request, c vaultCreds) bool {
	subID, key := readDeviceHeaders(r)
	if err := s.vaults.CheckDevice(c.LookupID, c.AuthSecret, subID, key); err != nil {
		s.vaultError(w, r, c.LookupID, err)
		return true
	}
	return false
}

func (s *Service) requireCreds(w http.ResponseWriter, r *http.Request, body *vaultCreds) (vaultCreds, bool) {
	c, ok := s.creds(r, body)
	if !ok {
		writeError(w, http.StatusBadRequest, "vault credentials required")
	}
	return c, ok
}

func (s *Service) admit(w http.ResponseWriter, r *http.Request, c vaultCreds, writes, device bool) bool {
	if s.limited(w, r, c.LookupID) {
		return false
	}
	if writes && s.watermarked(w) {
		return false
	}
	if device && s.deviceRefused(w, r, c) {
		return false
	}
	return true
}

func (s *Service) signatureRefused(w http.ResponseWriter, ts int64, verified func() bool) bool {
	if !freshTS(ts, s.lim.AuthTimestampSkew.Std()) {
		writeErrorCode(w, http.StatusBadRequest, CodeStaleTimestamp, "stale timestamp")
		return true
	}
	if !verified() {
		writeErrorCode(w, http.StatusUnauthorized, CodeBadSignature, "bad signature")
		return true
	}
	return false
}

func (s *Service) requireIdentity(w http.ResponseWriter, r *http.Request, signed func(identityHeaders) string) (identityHeaders, bool) {
	h, ok := readIdentityHeaders(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "identity headers required")
		return h, false
	}
	if !h.verify(signed(h), s.lim.AuthTimestampSkew.Std()) {
		writeErrorCode(w, http.StatusUnauthorized, CodeBadSignature, "bad signature")
		return h, false
	}
	return h, true
}
