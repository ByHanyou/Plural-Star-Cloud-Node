// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"errors"
	"log/slog"
	"os"
)

func listDir(dir string) []os.DirEntry {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.Warn("cloud: reading directory failed", "dir", dir, "err", err)
		}
		return nil
	}
	return entries
}

func removeFile(path string) bool {
	err := os.Remove(path)
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return true
	}
	slog.Warn("cloud: removing file failed", "path", path, "err", err)
	return false
}
