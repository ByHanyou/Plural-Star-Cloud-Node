// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build !windows

package cloud

import (
	"log/slog"

	"golang.org/x/sys/unix"
)

func diskUsage(path string) (used, free, total uint64) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		slog.Warn("cloud: reading disk usage failed", "path", path, "err", err)
		return 0, 0, 0
	}
	bsize := uint64(st.Bsize)
	total = uint64(st.Blocks) * bsize
	free = uint64(st.Bavail) * bsize
	if blocks := uint64(st.Blocks); blocks >= uint64(st.Bfree) {
		used = (blocks - uint64(st.Bfree)) * bsize
	}
	return used, free, total
}
