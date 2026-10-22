// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"encoding/base64"
	"net/http"
	"strconv"
)

func (s *Service) handleVaultLookup(w http.ResponseWriter, r *http.Request) {
	var body struct {
		LookupID string `json:"lookup_id"`
	}
	if !decodeJSON(w, r, int64(s.lim.MaxJSONBodyBytes), &body) {
		return
	}
	id := normalHex(body.LookupID)
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
	if !decodeJSON(w, r, int64(s.lim.MaxJSONBodyBytes), &body) {
		return
	}
	c, ok := s.creds(r, &body.vaultCreds)
	if !ok {
		writeError(w, http.StatusBadRequest, "lookup_id and auth_secret must be 64 hex characters")
		return
	}
	if wrappedKeyInvalid(w, body.WrappedMasterKey) {
		return
	}
	if !s.admit(w, r, c, true, false) {
		return
	}
	s.limiter.record(clientIP(r), c.LookupID)
	if err := s.vaults.Create(c.LookupID, c.AuthSecret, body.WrappedMasterKey); err != nil {
		s.vaultError(w, r, c.LookupID, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"created": true})
}

func wrappedKeyInvalid(w http.ResponseWriter, key string) bool {
	if key == "" {
		writeError(w, http.StatusBadRequest, "wrapped_master_key is required")
		return true
	}
	if _, err := base64.StdEncoding.DecodeString(key); err != nil {
		writeError(w, http.StatusBadRequest, "wrapped_master_key must be base64")
		return true
	}
	return false
}

func (s *Service) handleVaultInfo(w http.ResponseWriter, r *http.Request) {
	c, ok := s.requireCreds(w, r, nil)
	if !ok {
		return
	}
	if !s.admit(w, r, c, false, true) {
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
	c, ok := s.requireCreds(w, r, nil)
	if !ok {
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
	if !s.admit(w, r, c, false, true) {
		return
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
	c, ok := s.requireCreds(w, r, nil)
	if !ok {
		return
	}
	var body struct {
		ExpectedVersion int64    `json:"expected_version"`
		Ciphertext      []byte   `json:"ciphertext"`
		Objects         []ObjRef `json:"objects"`
	}
	if !decodeJSON(w, r, int64(s.lim.MaxManifestBodyBytes), &body) {
		return
	}
	if len(body.Ciphertext) == 0 {
		writeError(w, http.StatusBadRequest, "ciphertext is required")
		return
	}
	if !s.admit(w, r, c, true, true) {
		return
	}
	version, err := s.vaults.PutManifest(c.LookupID, c.AuthSecret, body.ExpectedVersion, body.Ciphertext, body.Objects)
	if err != nil {
		s.vaultError(w, r, c.LookupID, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": version})
}

func (s *Service) handleRewrap(w http.ResponseWriter, r *http.Request) {
	var body struct {
		vaultCreds
		NewLookupID      string `json:"new_lookup_id"`
		NewAuthSecret    string `json:"new_auth_secret"`
		WrappedMasterKey string `json:"wrapped_master_key"`
	}
	if !decodeJSON(w, r, int64(s.lim.MaxJSONBodyBytes), &body) {
		return
	}
	c, ok := s.requireCreds(w, r, &body.vaultCreds)
	if !ok {
		return
	}
	newID := normalHex(body.NewLookupID)
	newSecret := normalHex(body.NewAuthSecret)
	if newID != "" && !validHex32(newID) {
		writeError(w, http.StatusBadRequest, "new_lookup_id must be 64 hex characters")
		return
	}
	if newSecret != "" && !validHex32(newSecret) {
		writeError(w, http.StatusBadRequest, "new_auth_secret must be 64 hex characters")
		return
	}
	if wrappedKeyInvalid(w, body.WrappedMasterKey) {
		return
	}
	if !s.admit(w, r, c, false, true) {
		return
	}
	if err := s.vaults.Rewrap(c.LookupID, c.AuthSecret, newID, newSecret, body.WrappedMasterKey); err != nil {
		s.vaultError(w, r, c.LookupID, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rewrapped": true})
}
