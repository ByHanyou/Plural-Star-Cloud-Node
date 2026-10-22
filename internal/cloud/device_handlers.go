// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"net/http"
	"strconv"
	"strings"
)

const (
	maxSubIDLen         = 128
	maxDeviceLabelRunes = 128
)

func requireSubID(w http.ResponseWriter, r *http.Request) (string, bool) {
	subID := strings.TrimSpace(r.PathValue("sub_id"))
	if subID == "" {
		writeError(w, http.StatusBadRequest, "sub_id is required")
	}
	return subID, subID != ""
}

func (s *Service) handleDeviceLink(w http.ResponseWriter, r *http.Request) {
	var body struct {
		vaultCreds
		SubID string `json:"sub_id"`
		Label string `json:"label"`
	}
	if !decodeJSON(w, r, int64(s.lim.MaxJSONBodyBytes), &body) {
		return
	}
	c, ok := s.requireCreds(w, r, &body.vaultCreds)
	if !ok {
		return
	}
	body.SubID = strings.TrimSpace(body.SubID)
	if body.SubID == "" {
		writeError(w, http.StatusBadRequest, "sub_id is required")
		return
	}
	if len(body.SubID) > maxSubIDLen {
		writeError(w, http.StatusBadRequest, "sub_id must be at most "+strconv.Itoa(maxSubIDLen)+" bytes")
		return
	}
	if runes := []rune(body.Label); len(runes) > maxDeviceLabelRunes {
		body.Label = string(runes[:maxDeviceLabelRunes])
	}
	if s.limited(w, r, c.LookupID) {
		return
	}
	devices, key, existed, err := s.vaults.LinkDevice(c.LookupID, c.AuthSecret, body.SubID, body.Label)
	if err != nil {
		s.vaultError(w, r, c.LookupID, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"devices":    devices,
		"device_key": key,
		"existed":    existed,
	})
}

func (s *Service) handleDeviceUnlink(w http.ResponseWriter, r *http.Request) {
	c, ok := s.requireCreds(w, r, nil)
	if !ok {
		return
	}
	subID, ok := requireSubID(w, r)
	if !ok {
		return
	}
	if !s.admit(w, r, c, false, true) {
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

func (s *Service) handleDeviceKey(w http.ResponseWriter, r *http.Request) {
	var body vaultCreds
	if r.ContentLength != 0 && !decodeJSON(w, r, int64(s.lim.MaxJSONBodyBytes), &body) {
		return
	}
	c, ok := s.requireCreds(w, r, &body)
	if !ok {
		return
	}
	subID, ok := requireSubID(w, r)
	if !ok {
		return
	}
	if s.limited(w, r, c.LookupID) {
		return
	}
	key, err := s.vaults.IssueDeviceKey(c.LookupID, c.AuthSecret, subID)
	if err != nil {
		s.vaultError(w, r, c.LookupID, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"device_key": key})
}
