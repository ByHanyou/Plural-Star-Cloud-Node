// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
)

type remoteBackend struct {
	base   string
	secret string
	client *http.Client
}

func newRemoteBackend(baseURL, secret string) *remoteBackend {
	return &remoteBackend{
		base:   strings.TrimRight(baseURL, "/"),
		secret: secret,
		client: &http.Client{Timeout: 10 * time.Minute},
	}
}

func (b *remoteBackend) Name() string { return config.MediaBackendRemote }

func (b *remoteBackend) request(method, path string, body io.Reader, size int64) (*http.Response, error) {
	req, err := http.NewRequest(method, b.base+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+b.secret)
	if size >= 0 && body != nil {
		req.ContentLength = size
	}
	return b.client.Do(req)
}

func (b *remoteBackend) Put(id string, r io.Reader, size int64) error {
	resp, err := b.request(http.MethodPut, "/cloud/blob/"+id, r, size)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("remote blob put %s: status %d", id, resp.StatusCode)
	}
	return nil
}

func (b *remoteBackend) PutFile(id string, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	err = b.Put(id, f, st.Size())
	_ = f.Close()
	if err != nil {
		return err
	}
	return os.Remove(path)
}

func (b *remoteBackend) Get(id string) (io.ReadCloser, int64, error) {
	resp, err := b.request(http.MethodGet, "/cloud/blob/"+id, nil, -1)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode == http.StatusNotFound {
		_ = resp.Body.Close()
		return nil, 0, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, 0, fmt.Errorf("remote blob get %s: status %d", id, resp.StatusCode)
	}
	return resp.Body, resp.ContentLength, nil
}

func (b *remoteBackend) Head(id string) (int64, error) {
	resp, err := b.request(http.MethodHead, "/cloud/blob/"+id, nil, -1)
	if err != nil {
		return 0, err
	}
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return 0, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("remote blob head %s: status %d", id, resp.StatusCode)
	}
	size, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	return size, nil
}

func (b *remoteBackend) Delete(id string) error {
	resp, err := b.request(http.MethodDelete, "/cloud/blob/"+id, nil, -1)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("remote blob delete %s: status %d", id, resp.StatusCode)
	}
	return nil
}

type blobListEntry struct {
	ID       string `json:"id"`
	Size     int64  `json:"size"`
	Modified int64  `json:"modified"`
}

func (b *remoteBackend) List(fn func(id string, size int64, modified time.Time) error) error {
	resp, err := b.request(http.MethodGet, "/cloud/blob", nil, -1)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("remote blob list: status %d", resp.StatusCode)
	}
	var entries []blobListEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return err
	}
	for _, e := range entries {
		if !validHex32(e.ID) {
			continue
		}
		if err := fn(e.ID, e.Size, time.UnixMilli(e.Modified)); err != nil {
			return err
		}
	}
	return nil
}
