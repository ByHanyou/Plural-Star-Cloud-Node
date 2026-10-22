// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type pushEnvelope struct {
	Sealed       string `json:"sealed"`
	SenderBoxPub string `json:"sender_box_pub"`
}

func (s *Service) pushRequest(w http.ResponseWriter, r *http.Request, req any) ([]byte, bool) {
	if !s.push.enabled() {
		writeErrorCode(w, http.StatusServiceUnavailable, CodePushDisabled, ErrPushDisabled.Error())
		return nil, false
	}
	r.Body = http.MaxBytesReader(w, r.Body, int64(s.lim.PushMaxBodyBytes))
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeBodyError(w, err, "body unreadable")
		return nil, false
	}
	body := raw
	var env pushEnvelope
	if json.Unmarshal(raw, &env) == nil && env.Sealed != "" {
		plain, err := s.push.unseal(env.Sealed, env.SenderBoxPub)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return nil, false
		}
		body = plain
	}
	if err := json.Unmarshal(body, req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return nil, false
	}
	return body, true
}

func (s *Service) queuePush(w http.ResponseWriter, kind string, body []byte) {
	if s.watermarked(w) {
		return
	}
	if err := s.push.enqueue(kind, body); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"queued": true})
}

func (s *Service) handlePushRegister(w http.ResponseWriter, r *http.Request) {
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
	body, ok := s.pushRequest(w, r, &req)
	if !ok {
		return
	}
	if s.signatureRefused(w, req.TS, func() bool {
		signedNew := fmt.Sprintf("psgw-register|%s|%d|%s|%s|%s|%s|%s",
			req.PeerID, req.TS, req.Env, req.ActivityToken, req.DeviceToken,
			strings.Join(req.Watch, ","), strings.Join(req.Pinned, ","))
		signedOld := fmt.Sprintf("psgw-register|%s|%d|%s|%s|%s",
			req.PeerID, req.TS, req.Env, req.ActivityToken, strings.Join(req.Watch, ","))
		return verifySigned(req.PeerID, req.EdPub, req.Sig, signedNew) ||
			verifySigned(req.PeerID, req.EdPub, req.Sig, signedOld)
	}) {
		return
	}
	s.queuePush(w, pushKindRegister, body)
}

func (s *Service) handlePushEvent(w http.ResponseWriter, r *http.Request) {
	var req frontRequest
	body, ok := s.pushRequest(w, r, &req)
	if !ok {
		return
	}
	if s.signatureRefused(w, req.TS, req.verify) {
		return
	}
	s.queuePush(w, pushKindEvent, body)
}
