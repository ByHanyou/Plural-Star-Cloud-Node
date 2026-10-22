// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"crypto/subtle"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

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
	id, ok := requireObjectID(w, r)
	if !ok {
		return
	}
	if r.ContentLength > s.objects.maxBytes() {
		writeErrorCode(w, http.StatusRequestEntityTooLarge, CodeObjectTooLarge, "object too large")
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
	n, err := io.Copy(f, io.LimitReader(r.Body, s.objects.maxBytes()+1))
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp)
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if n > s.objects.maxBytes() {
		_ = os.Remove(tmp)
		writeErrorCode(w, http.StatusRequestEntityTooLarge, CodeObjectTooLarge, "object too large")
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
	id, ok := requireObjectID(w, r)
	if !ok {
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
	writeObject(w, rc, size)
}

func (s *Service) handleBlobHead(w http.ResponseWriter, r *http.Request) {
	id, ok := objectID(r)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	size, err := s.objects.local.Head(id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		return
	}
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(http.StatusOK)
}

func (s *Service) handleBlobDelete(w http.ResponseWriter, r *http.Request) {
	id, ok := requireObjectID(w, r)
	if !ok {
		return
	}
	if err := s.objects.local.Delete(id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
