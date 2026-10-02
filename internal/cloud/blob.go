// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
)

var ErrNotFound = errors.New("not found")

type Backend interface {
	Name() string
	Put(id string, r io.Reader, size int64) error
	PutFile(id string, path string) error
	Get(id string) (io.ReadCloser, int64, error)
	Head(id string) (int64, error)
	Delete(id string) error
	List(fn func(id string, size int64, modified time.Time) error) error
}

func newBackend(cfg config.CloudConfig, localDir string) (Backend, error) {
	switch cfg.MediaBackend {
	case config.MediaBackendLocal, "":
		return newLocalBackend(localDir), nil
	case config.MediaBackendRemote:
		return newRemoteBackend(cfg.RemoteBlobURL, cfg.RemoteBlobSecret), nil
	case config.MediaBackendS3:
		return newS3Backend(cfg), nil
	default:
		return nil, fmt.Errorf("cloud: unknown media backend %q", cfg.MediaBackend)
	}
}

type localBackend struct {
	dir string
}

func newLocalBackend(dir string) *localBackend { return &localBackend{dir: dir} }

func (b *localBackend) Name() string { return config.MediaBackendLocal }

func (b *localBackend) path(id string) string { return filepath.Join(b.dir, id) }

func (b *localBackend) Put(id string, r io.Reader, size int64) error {
	tmp := b.path(id) + ".put"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	n, err := io.Copy(f, r)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if size >= 0 && n != size {
		_ = os.Remove(tmp)
		return fmt.Errorf("blob %s: wrote %d bytes, expected %d", id, n, size)
	}
	return renameRetry(tmp, b.path(id))
}

func (b *localBackend) PutFile(id string, path string) error {
	if err := renameRetry(path, b.path(id)); err == nil {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if err := b.Put(id, f, st.Size()); err != nil {
		return err
	}
	return os.Remove(path)
}

func (b *localBackend) Get(id string) (io.ReadCloser, int64, error) {
	f, err := os.Open(b.path(id))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, ErrNotFound
		}
		return nil, 0, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, err
	}
	return f, st.Size(), nil
}

func (b *localBackend) Head(id string) (int64, error) {
	st, err := os.Stat(b.path(id))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, ErrNotFound
		}
		return 0, err
	}
	return st.Size(), nil
}

func (b *localBackend) Delete(id string) error {
	err := os.Remove(b.path(id))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (b *localBackend) List(fn func(id string, size int64, modified time.Time) error) error {
	entries, err := os.ReadDir(b.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !validHex32(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if err := fn(e.Name(), info.Size(), info.ModTime()); err != nil {
			return err
		}
	}
	return nil
}

func renameRetry(from, to string) error {
	var err error
	for i := 0; i < 10; i++ {
		err = os.Rename(from, to)
		if err == nil {
			return nil
		}
		time.Sleep(time.Duration(50*(i+1)) * time.Millisecond)
	}
	return err
}
