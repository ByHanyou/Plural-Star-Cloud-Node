// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
)

func objectID(r *http.Request) (string, bool) {
	id := strings.ToLower(r.PathValue("id"))
	return id, validHex32(id)
}

func requireObjectID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id, ok := objectID(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid object id")
	}
	return id, ok
}

func writeObject(w http.ResponseWriter, rc io.ReadCloser, size int64) {
	defer rc.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	if size >= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	}
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, rc)
}

func (s *Service) handleObjectHead(w http.ResponseWriter, r *http.Request) {
	c, ok := s.requireCreds(w, r, nil)
	if !ok {
		return
	}
	id, ok := requireObjectID(w, r)
	if !ok {
		return
	}
	if !s.admit(w, r, c, false, true) {
		return
	}
	owns, err := s.vaults.Owns(c.LookupID, c.AuthSecret, id)
	if err != nil {
		s.vaultError(w, r, c.LookupID, err)
		return
	}
	if owns {
		size, _, herr := s.objects.Head(id)
		if herr == nil {
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			w.WriteHeader(http.StatusOK)
			return
		}
		if !errors.Is(herr, ErrNotFound) {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
	if received, pending := s.objects.Received(c.LookupID, id); pending {
		w.Header().Set(headerUploadOffs, strconv.FormatInt(received, 10))
	}
	w.WriteHeader(http.StatusNotFound)
}

func (s *Service) handleObjectGet(w http.ResponseWriter, r *http.Request) {
	c, ok := s.requireCreds(w, r, nil)
	if !ok {
		return
	}
	id, ok := requireObjectID(w, r)
	if !ok {
		return
	}
	if !s.admit(w, r, c, false, true) {
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
	writeObject(w, rc, size)
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
	c, ok := s.requireCreds(w, r, nil)
	if !ok {
		return
	}
	id, ok := requireObjectID(w, r)
	if !ok {
		return
	}
	tier := strings.ToLower(strings.TrimSpace(r.Header.Get(headerTier)))
	if tier == "" {
		tier = TierBase
	}
	if !validTier(tier) {
		writeError(w, http.StatusBadRequest, "X-Ps-Tier must be base or media")
		return
	}
	start, total, err := parseContentRange(r.Header.Get("Content-Range"), r.ContentLength)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if total > s.objects.maxBytes() {
		s.vaultError(w, r, c.LookupID, ErrTooLarge)
		return
	}
	if !s.admit(w, r, c, true, true) {
		return
	}
	owns, err := s.vaults.Owns(c.LookupID, c.AuthSecret, id)
	if err != nil {
		s.vaultError(w, r, c.LookupID, err)
		return
	}
	if owns {
		if size, existingTier, herr := s.objects.Head(id); herr == nil {
			if err := s.vaults.CommitObject(c.LookupID, c.AuthSecret, ObjRef{ID: id, Tier: existingTier, Size: size}); err != nil {
				s.vaultError(w, r, c.LookupID, err)
				return
			}
			_, _ = io.Copy(io.Discard, r.Body)
			writeJSON(w, http.StatusOK, map[string]any{"complete": true, "exists": true, "size": size})
			return
		}
	}
	if start == 0 {
		if err := s.vaults.ReserveObject(c.LookupID, c.AuthSecret, id, tier, total); err != nil {
			s.vaultError(w, r, c.LookupID, err)
			return
		}
	}
	res, err := s.objects.Append(c.LookupID, id, tier, start, total, r.Body)
	if err != nil {
		if errors.Is(err, ErrOffset) {
			w.Header().Set(headerUploadOffs, strconv.FormatInt(res.Received, 10))
			writeJSON(w, http.StatusConflict, map[string]any{"error": "upload offset mismatch", "code": CodeUploadOffsetMismatch, "received": res.Received})
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
