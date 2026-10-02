// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	maxJSONBody     = 64 << 10
	maxManifestBody = 32 << 20
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
	writeJSON(w, status, map[string]string{"error": msg})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, limit int64, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	return true
}

type vaultCreds struct {
	LookupID   string `json:"lookup_id"`
	AuthSecret string `json:"auth_secret"`
}

func (s *Service) creds(r *http.Request, body *vaultCreds) (vaultCreds, bool) {
	c := vaultCreds{
		LookupID:   strings.ToLower(strings.TrimSpace(r.Header.Get(headerVaultID))),
		AuthSecret: strings.ToLower(strings.TrimSpace(r.Header.Get(headerVaultAuth))),
	}
	if body != nil {
		if c.LookupID == "" {
			c.LookupID = strings.ToLower(strings.TrimSpace(body.LookupID))
		}
		if c.AuthSecret == "" {
			c.AuthSecret = strings.ToLower(strings.TrimSpace(body.AuthSecret))
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
		writeError(w, http.StatusNotFound, "no vault")
	case errors.Is(err, ErrBadAuth):
		s.limiter.record(clientIP(r), lookupID)
		writeError(w, http.StatusUnauthorized, "bad auth secret")
	case errors.Is(err, ErrVaultExists):
		writeError(w, http.StatusConflict, "vault already exists")
	case errors.As(err, &verr):
		writeJSON(w, http.StatusConflict, map[string]any{"error": "manifest version conflict", "version": verr.Current})
	case errors.As(err, &merr):
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "manifest references missing objects", "missing": merr.IDs})
	case errors.Is(err, ErrQuota):
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{
			"error":            "vault quota exceeded",
			"base_quota_bytes": int64(s.cfg.BaseQuotaMB) << 20,
			"media_quota_bytes": int64(s.cfg.MediaQuotaMB) << 20,
		})
	case errors.Is(err, ErrTooLarge):
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{
			"error":     "object too large",
			"max_bytes": int64(s.cfg.ObjectMaxMB) << 20,
		})
	case errors.Is(err, ErrHashMismatch):
		writeError(w, http.StatusBadRequest, "object id does not match ciphertext hash")
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, "not found")
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func (s *Service) handleVaultLookup(w http.ResponseWriter, r *http.Request) {
	var body struct {
		LookupID string `json:"lookup_id"`
	}
	if !decodeJSON(w, r, maxJSONBody, &body) {
		return
	}
	id := strings.ToLower(strings.TrimSpace(body.LookupID))
	if !validHex32(id) {
		writeError(w, http.StatusBadRequest, "lookup_id must be 64 hex characters")
		return
	}
	if s.limited(w, r, id) {
		return
	}
	s.limiter.record(clientIP(r), id)
	writeJSON(w, http.StatusOK, map[string]bool{"exists": s.vaults.exists(id)})
}

func (s *Service) handleVaultCreate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		vaultCreds
		WrappedMasterKey string `json:"wrapped_master_key"`
	}
	if !decodeJSON(w, r, maxJSONBody, &body) {
		return
	}
	c, ok := s.creds(r, &body.vaultCreds)
	if !ok {
		writeError(w, http.StatusBadRequest, "lookup_id and auth_secret must be 64 hex characters")
		return
	}
	if body.WrappedMasterKey == "" {
		writeError(w, http.StatusBadRequest, "wrapped_master_key is required")
		return
	}
	if _, err := base64.StdEncoding.DecodeString(body.WrappedMasterKey); err != nil {
		writeError(w, http.StatusBadRequest, "wrapped_master_key must be base64")
		return
	}
	if s.limited(w, r, c.LookupID) {
		return
	}
	if s.watermarked(w) {
		return
	}
	if err := s.vaults.Create(c.LookupID, c.AuthSecret, body.WrappedMasterKey); err != nil {
		s.vaultError(w, r, c.LookupID, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"created": true})
}

func (s *Service) handleVaultInfo(w http.ResponseWriter, r *http.Request) {
	c, ok := s.creds(r, nil)
	if !ok {
		writeError(w, http.StatusBadRequest, "vault credentials required")
		return
	}
	if s.limited(w, r, c.LookupID) {
		return
	}
	info, err := s.vaults.Info(c.LookupID, c.AuthSecret)
	if err != nil {
		s.vaultError(w, r, c.LookupID, err)
		return
	}
	writeJSON(w, http.StatusOK, info)
}

func (s *Service) handleManifestGet(w http.ResponseWriter, r *http.Request) {
	c, ok := s.creds(r, nil)
	if !ok {
		writeError(w, http.StatusBadRequest, "vault credentials required")
		return
	}
	if s.limited(w, r, c.LookupID) {
		return
	}
	var version int64
	if v := r.URL.Query().Get("version"); v != "" {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil || parsed < 0 {
			writeError(w, http.StatusBadRequest, "invalid version")
			return
		}
		version = parsed
	}
	m, wrapped, err := s.vaults.GetManifest(c.LookupID, c.AuthSecret, version)
	if err != nil {
		s.vaultError(w, r, c.LookupID, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"version":            m.Version,
		"ciphertext":         m.Ciphertext,
		"objects":            m.Objects,
		"wrapped_master_key": wrapped,
	})
}

func (s *Service) handleManifestPut(w http.ResponseWriter, r *http.Request) {
	c, ok := s.creds(r, nil)
	if !ok {
		writeError(w, http.StatusBadRequest, "vault credentials required")
		return
	}
	if s.limited(w, r, c.LookupID) {
		return
	}
	var body struct {
		ExpectedVersion int64    `json:"expected_version"`
		Ciphertext      []byte   `json:"ciphertext"`
		Objects         []ObjRef `json:"objects"`
	}
	if !decodeJSON(w, r, maxManifestBody, &body) {
		return
	}
	if len(body.Ciphertext) == 0 {
		writeError(w, http.StatusBadRequest, "ciphertext is required")
		return
	}
	if s.watermarked(w) {
		return
	}
	version, err := s.vaults.PutManifest(c.LookupID, c.AuthSecret, body.ExpectedVersion, body.Ciphertext, body.Objects)
	if err != nil {
		s.vaultError(w, r, c.LookupID, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": version})
}

func objectID(r *http.Request) (string, bool) {
	id := strings.ToLower(r.PathValue("id"))
	return id, validHex32(id)
}

func (s *Service) handleObjectHead(w http.ResponseWriter, r *http.Request) {
	c, ok := s.creds(r, nil)
	if !ok {
		writeError(w, http.StatusBadRequest, "vault credentials required")
		return
	}
	id, ok := objectID(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid object id")
		return
	}
	if s.limited(w, r, c.LookupID) {
		return
	}
	owns, err := s.vaults.Owns(c.LookupID, c.AuthSecret, id)
	if err != nil {
		s.vaultError(w, r, c.LookupID, err)
		return
	}
	if owns {
		if size, _, herr := s.objects.Head(id); herr == nil {
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			w.WriteHeader(http.StatusOK)
			return
		}
	}
	if received, pending := s.objects.Received(c.LookupID, id); pending {
		w.Header().Set(headerUploadOffs, strconv.FormatInt(received, 10))
	}
	w.WriteHeader(http.StatusNotFound)
}

func (s *Service) handleObjectGet(w http.ResponseWriter, r *http.Request) {
	c, ok := s.creds(r, nil)
	if !ok {
		writeError(w, http.StatusBadRequest, "vault credentials required")
		return
	}
	id, ok := objectID(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid object id")
		return
	}
	if s.limited(w, r, c.LookupID) {
		return
	}
	owns, err := s.vaults.Owns(c.LookupID, c.AuthSecret, id)
	if err != nil {
		s.vaultError(w, r, c.LookupID, err)
		return
	}
	if !owns {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	rc, size, err := s.objects.Get(id)
	if err != nil {
		s.vaultError(w, r, c.LookupID, err)
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	if size >= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	}
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, rc)
}

func parseContentRange(h string, contentLength int64) (start, total int64, err error) {
	if h == "" {
		if contentLength <= 0 {
			return 0, 0, errors.New("Content-Length or Content-Range required")
		}
		return 0, contentLength, nil
	}
	h = strings.TrimSpace(h)
	if !strings.HasPrefix(h, "bytes ") {
		return 0, 0, errors.New("Content-Range must be bytes start-end/total")
	}
	rest := strings.TrimPrefix(h, "bytes ")
	slash := strings.IndexByte(rest, '/')
	dash := strings.IndexByte(rest, '-')
	if slash < 0 || dash < 0 || dash > slash {
		return 0, 0, errors.New("Content-Range must be bytes start-end/total")
	}
	start, err = strconv.ParseInt(rest[:dash], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	end, err := strconv.ParseInt(rest[dash+1:slash], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	total, err = strconv.ParseInt(rest[slash+1:], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	if start < 0 || end < start || total <= 0 || end >= total {
		return 0, 0, errors.New("Content-Range out of bounds")
	}
	if contentLength >= 0 && contentLength != end-start+1 {
		return 0, 0, errors.New("Content-Length does not match Content-Range")
	}
	return start, total, nil
}

func (s *Service) handleObjectPut(w http.ResponseWriter, r *http.Request) {
	c, ok := s.creds(r, nil)
	if !ok {
		writeError(w, http.StatusBadRequest, "vault credentials required")
		return
	}
	id, ok := objectID(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid object id")
		return
	}
	if s.limited(w, r, c.LookupID) {
		return
	}
	tier := strings.ToLower(strings.TrimSpace(r.Header.Get(headerTier)))
	if tier == "" {
		tier = TierBase
	}
	if tier != TierBase && tier != TierMedia {
		writeError(w, http.StatusBadRequest, "X-Ps-Tier must be base or media")
		return
	}
	start, total, err := parseContentRange(r.Header.Get("Content-Range"), r.ContentLength)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if total > int64(s.cfg.ObjectMaxMB)<<20 {
		s.vaultError(w, r, c.LookupID, ErrTooLarge)
		return
	}
	if s.watermarked(w) {
		return
	}
	if size, existingTier, herr := s.objects.Head(id); herr == nil {
		if err := s.vaults.CommitObject(c.LookupID, c.AuthSecret, ObjRef{ID: id, Tier: existingTier, Size: size}); err != nil {
			s.vaultError(w, r, c.LookupID, err)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		writeJSON(w, http.StatusOK, map[string]any{"complete": true, "exists": true, "size": size})
		return
	}
	if start == 0 {
		if err := s.vaults.ReserveObject(c.LookupID, c.AuthSecret, id, tier, total); err != nil {
			s.vaultError(w, r, c.LookupID, err)
			return
		}
	} else if _, err := s.vaults.Info(c.LookupID, c.AuthSecret); err != nil {
		s.vaultError(w, r, c.LookupID, err)
		return
	}
	res, err := s.objects.Append(c.LookupID, id, tier, start, total, r.Body)
	if err != nil {
		if errors.Is(err, ErrOffset) {
			w.Header().Set(headerUploadOffs, strconv.FormatInt(res.Received, 10))
			writeJSON(w, http.StatusConflict, map[string]any{"error": "upload offset mismatch", "received": res.Received})
			return
		}
		s.vaultError(w, r, c.LookupID, err)
		return
	}
	if !res.Complete {
		w.Header().Set(headerUploadOffs, strconv.FormatInt(res.Received, 10))
		writeJSON(w, http.StatusAccepted, map[string]any{"complete": false, "received": res.Received, "total": res.Total})
		return
	}
	if err := s.vaults.CommitObject(c.LookupID, c.AuthSecret, ObjRef{ID: id, Tier: tier, Size: total}); err != nil {
		s.vaultError(w, r, c.LookupID, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"complete": true, "size": total})
}

func (s *Service) handleDeviceLink(w http.ResponseWriter, r *http.Request) {
	var body struct {
		vaultCreds
		SubID string `json:"sub_id"`
		Label string `json:"label"`
	}
	if !decodeJSON(w, r, maxJSONBody, &body) {
		return
	}
	c, ok := s.creds(r, &body.vaultCreds)
	if !ok {
		writeError(w, http.StatusBadRequest, "vault credentials required")
		return
	}
	body.SubID = strings.TrimSpace(body.SubID)
	if body.SubID == "" || len(body.SubID) > 128 {
		writeError(w, http.StatusBadRequest, "sub_id is required")
		return
	}
	if len(body.Label) > 128 {
		body.Label = body.Label[:128]
	}
	if s.limited(w, r, c.LookupID) {
		return
	}
	devices, err := s.vaults.LinkDevice(c.LookupID, c.AuthSecret, body.SubID, body.Label)
	if err != nil {
		s.vaultError(w, r, c.LookupID, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"devices": devices})
}

func (s *Service) handleDeviceUnlink(w http.ResponseWriter, r *http.Request) {
	c, ok := s.creds(r, nil)
	if !ok {
		writeError(w, http.StatusBadRequest, "vault credentials required")
		return
	}
	subID := strings.TrimSpace(r.PathValue("sub_id"))
	if subID == "" {
		writeError(w, http.StatusBadRequest, "sub_id is required")
		return
	}
	if s.limited(w, r, c.LookupID) {
		return
	}
	devices, err := s.vaults.UnlinkDevice(c.LookupID, c.AuthSecret, subID)
	if err != nil {
		s.vaultError(w, r, c.LookupID, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"devices":    devices,
		"grace_days": s.cfg.GraceDays,
	})
}

func (s *Service) handleRewrap(w http.ResponseWriter, r *http.Request) {
	var body struct {
		vaultCreds
		NewLookupID      string `json:"new_lookup_id"`
		NewAuthSecret    string `json:"new_auth_secret"`
		WrappedMasterKey string `json:"wrapped_master_key"`
	}
	if !decodeJSON(w, r, maxJSONBody, &body) {
		return
	}
	c, ok := s.creds(r, &body.vaultCreds)
	if !ok {
		writeError(w, http.StatusBadRequest, "vault credentials required")
		return
	}
	newID := strings.ToLower(strings.TrimSpace(body.NewLookupID))
	newSecret := strings.ToLower(strings.TrimSpace(body.NewAuthSecret))
	if newID != "" && !validHex32(newID) {
		writeError(w, http.StatusBadRequest, "new_lookup_id must be 64 hex characters")
		return
	}
	if newSecret != "" && !validHex32(newSecret) {
		writeError(w, http.StatusBadRequest, "new_auth_secret must be 64 hex characters")
		return
	}
	if body.WrappedMasterKey == "" {
		writeError(w, http.StatusBadRequest, "wrapped_master_key is required")
		return
	}
	if _, err := base64.StdEncoding.DecodeString(body.WrappedMasterKey); err != nil {
		writeError(w, http.StatusBadRequest, "wrapped_master_key must be base64")
		return
	}
	if s.limited(w, r, c.LookupID) {
		return
	}
	if err := s.vaults.Rewrap(c.LookupID, c.AuthSecret, newID, newSecret, body.WrappedMasterKey); err != nil {
		s.vaultError(w, r, c.LookupID, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rewrapped": true})
}

func (s *Service) handleInboxGet(w http.ResponseWriter, r *http.Request) {
	h, ok := readIdentityHeaders(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "identity headers required")
		return
	}
	if !h.verify(fmt.Sprintf("pscloud-inbox|%s|%d", h.PeerID, h.TS)) {
		writeError(w, http.StatusUnauthorized, "bad signature")
		return
	}
	pid, _ := peer.Decode(h.PeerID)
	packets := s.inbox.Take(pid)
	out := make([]map[string]any, 0, len(packets))
	for _, p := range packets {
		sender := ""
		if sid, err := peer.IDFromBytes(p.SenderID); err == nil {
			sender = sid.String()
		}
		out = append(out, map[string]any{
			"packet_id":      hex.EncodeToString(p.ID[:]),
			"sender_peer_id": sender,
			"payload":        base64.StdEncoding.EncodeToString(p.Payload),
			"timestamp":      p.Timestamp,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"packets": out})
}

const (
	maxFrontersLen = 120
	maxNameLen     = 64
	maxReaders     = 500
)

func truncateRunes(s string, max int) string {
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max-1]) + "…"
}

func (s *Service) handleFrontPut(w http.ResponseWriter, r *http.Request) {
	target := r.PathValue("peer_id")
	if _, err := peer.Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, "invalid peer_id")
		return
	}
	var req struct {
		PeerID      string   `json:"peer_id"`
		EdPub       string   `json:"ed_pub"`
		Sig         string   `json:"sig"`
		TS          int64    `json:"ts"`
		Fronters    string   `json:"fronters"`
		StartTime   int64    `json:"start_time"`
		Name        string   `json:"name"`
		Primary     string   `json:"primary"`
		CoFront     string   `json:"co_front"`
		CoConscious string   `json:"co_conscious"`
		Readers     []string `json:"readers"`
	}
	if !decodeJSON(w, r, maxJSONBody, &req) {
		return
	}
	if req.PeerID != target {
		writeError(w, http.StatusBadRequest, "peer_id does not match path")
		return
	}
	if !freshTS(req.TS) {
		writeError(w, http.StatusBadRequest, "stale timestamp")
		return
	}
	signed := fmt.Sprintf("psgw-front|%s|%d|%s|%d|%s|%s|%s|%s|%s",
		req.PeerID, req.TS, req.Fronters, req.StartTime, req.Name,
		req.Primary, req.CoFront, req.CoConscious, strings.Join(req.Readers, ","))
	if !verifySigned(req.PeerID, req.EdPub, req.Sig, signed) {
		writeError(w, http.StatusUnauthorized, "bad signature")
		return
	}
	if len(req.Readers) > maxReaders {
		req.Readers = req.Readers[:maxReaders]
	}
	entry := FrontEntry{
		Fronters:    truncateRunes(req.Fronters, maxFrontersLen),
		StartTime:   req.StartTime,
		Name:        truncateRunes(req.Name, maxNameLen),
		At:          time.Now().UnixMilli(),
		AuthoredAt:  req.TS,
		Primary:     truncateRunes(req.Primary, maxFrontersLen),
		CoFront:     truncateRunes(req.CoFront, maxFrontersLen),
		CoConscious: truncateRunes(req.CoConscious, maxFrontersLen),
		Readers:     req.Readers,
	}
	if err := s.fronts.Put(req.PeerID, entry); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Service) handleFrontGet(w http.ResponseWriter, r *http.Request) {
	target := r.PathValue("peer_id")
	if _, err := peer.Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, "invalid peer_id")
		return
	}
	h, ok := readIdentityHeaders(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "identity headers required")
		return
	}
	if !h.verify(fmt.Sprintf("pscloud-front-get|%s|%d|%s", h.PeerID, h.TS, target)) {
		writeError(w, http.StatusUnauthorized, "bad signature")
		return
	}
	entry, found := s.fronts.Get(target)
	if !found || h.PeerID == target || !containsString(entry.Readers, h.PeerID) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"fronters":     entry.Fronters,
		"primary":      entry.Primary,
		"co_front":     entry.CoFront,
		"co_conscious": entry.CoConscious,
		"start_time":   entry.StartTime,
		"name":         entry.Name,
		"at":           entry.At,
		"authored_at":  entry.AuthoredAt,
	})
}

func containsString(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

type pushEnvelope struct {
	Sealed       string `json:"sealed"`
	SenderBoxPub string `json:"sender_box_pub"`
}

// pushBody returns the plaintext request the gateway will verify, opening a
// sealed envelope with this node's box key when the sender used one.
func (s *Service) pushBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, pushMaxBody)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "body too large or unreadable")
		return nil, false
	}
	var env pushEnvelope
	if json.Unmarshal(raw, &env) == nil && env.Sealed != "" {
		plain, err := s.push.unseal(env.Sealed, env.SenderBoxPub)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return nil, false
		}
		return plain, true
	}
	return raw, true
}

func (s *Service) handlePushRegister(w http.ResponseWriter, r *http.Request) {
	if !s.push.enabled() {
		writeError(w, http.StatusServiceUnavailable, ErrPushDisabled.Error())
		return
	}
	body, ok := s.pushBody(w, r)
	if !ok {
		return
	}
	var req struct {
		PeerID        string   `json:"peer_id"`
		EdPub         string   `json:"ed_pub"`
		Sig           string   `json:"sig"`
		TS            int64    `json:"ts"`
		Env           string   `json:"env"`
		ActivityToken string   `json:"activity_token"`
		DeviceToken   string   `json:"device_token"`
		Watch         []string `json:"watch"`
		Pinned        []string `json:"pinned"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if !freshTS(req.TS) {
		writeError(w, http.StatusBadRequest, "stale timestamp")
		return
	}
	signedNew := fmt.Sprintf("psgw-register|%s|%d|%s|%s|%s|%s|%s",
		req.PeerID, req.TS, req.Env, req.ActivityToken, req.DeviceToken,
		strings.Join(req.Watch, ","), strings.Join(req.Pinned, ","))
	signedOld := fmt.Sprintf("psgw-register|%s|%d|%s|%s|%s",
		req.PeerID, req.TS, req.Env, req.ActivityToken, strings.Join(req.Watch, ","))
	if !verifySigned(req.PeerID, req.EdPub, req.Sig, signedNew) &&
		!verifySigned(req.PeerID, req.EdPub, req.Sig, signedOld) {
		writeError(w, http.StatusUnauthorized, "bad signature")
		return
	}
	if err := s.push.enqueue(pushKindRegister, body); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"queued": true})
}

func (s *Service) handlePushEvent(w http.ResponseWriter, r *http.Request) {
	if !s.push.enabled() {
		writeError(w, http.StatusServiceUnavailable, ErrPushDisabled.Error())
		return
	}
	body, ok := s.pushBody(w, r)
	if !ok {
		return
	}
	var req struct {
		PeerID      string   `json:"peer_id"`
		EdPub       string   `json:"ed_pub"`
		Sig         string   `json:"sig"`
		TS          int64    `json:"ts"`
		Fronters    string   `json:"fronters"`
		StartTime   int64    `json:"start_time"`
		Name        string   `json:"name"`
		Primary     string   `json:"primary"`
		CoFront     string   `json:"co_front"`
		CoConscious string   `json:"co_conscious"`
		Readers     []string `json:"readers"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if !freshTS(req.TS) {
		writeError(w, http.StatusBadRequest, "stale timestamp")
		return
	}
	signedRead := fmt.Sprintf("psgw-front|%s|%d|%s|%d|%s|%s|%s|%s|%s",
		req.PeerID, req.TS, req.Fronters, req.StartTime, req.Name,
		req.Primary, req.CoFront, req.CoConscious, strings.Join(req.Readers, ","))
	signedNew := fmt.Sprintf("psgw-front|%s|%d|%s|%d|%s", req.PeerID, req.TS, req.Fronters, req.StartTime, req.Name)
	signedOld := fmt.Sprintf("psgw-front|%s|%d|%s|%d", req.PeerID, req.TS, req.Fronters, req.StartTime)
	if !verifySigned(req.PeerID, req.EdPub, req.Sig, signedRead) &&
		!verifySigned(req.PeerID, req.EdPub, req.Sig, signedNew) &&
		!verifySigned(req.PeerID, req.EdPub, req.Sig, signedOld) {
		writeError(w, http.StatusUnauthorized, "bad signature")
		return
	}
	if err := s.push.enqueue(pushKindEvent, body); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"queued": true})
}

func (s *Service) blobAuthed(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(got), []byte(s.cfg.BlobServeSecret)) != 1 {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next(w, r)
	}
}

func (s *Service) handleBlobList(w http.ResponseWriter, r *http.Request) {
	out := make([]blobListEntry, 0)
	err := s.objects.local.List(func(id string, size int64, modified time.Time) error {
		out = append(out, blobListEntry{ID: id, Size: size, Modified: modified.UnixMilli()})
		return nil
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Service) handleBlobPut(w http.ResponseWriter, r *http.Request) {
	id, ok := objectID(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid object id")
		return
	}
	if r.ContentLength > int64(s.cfg.ObjectMaxMB)<<20 {
		writeError(w, http.StatusRequestEntityTooLarge, "object too large")
		return
	}
	if s.watermarked(w) {
		return
	}
	if _, err := s.objects.local.Head(id); err == nil {
		_, _ = io.Copy(io.Discard, r.Body)
		writeJSON(w, http.StatusOK, map[string]bool{"exists": true})
		return
	}
	tmp := filepath.Join(s.objects.tmp, "blob."+id+".part")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	_, err = io.Copy(f, io.LimitReader(r.Body, (int64(s.cfg.ObjectMaxMB)<<20)+1))
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp)
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := s.objects.finalize(tmp, id, TierBase); err != nil {
		_ = os.Remove(tmp)
		if errors.Is(err, ErrHashMismatch) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]bool{"stored": true})
}

func (s *Service) handleBlobGet(w http.ResponseWriter, r *http.Request) {
	id, ok := objectID(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid object id")
		return
	}
	rc, size, err := s.objects.local.Get(id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, rc)
}

func (s *Service) handleBlobHead(w http.ResponseWriter, r *http.Request) {
	id, ok := objectID(r)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	size, err := s.objects.local.Head(id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(http.StatusOK)
}

func (s *Service) handleBlobDelete(w http.ResponseWriter, r *http.Request) {
	id, ok := objectID(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid object id")
		return
	}
	if err := s.objects.local.Delete(id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
