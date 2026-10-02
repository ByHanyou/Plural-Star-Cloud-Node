// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build !windows

package cloud

import "golang.org/x/sys/unix"

func diskUsage(path string) (used, free, total uint64) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, 0, 0
	}
	bsize := uint64(st.Bsize)
	total = uint64(st.Blocks) * bsize
	// Bavail is what an UNPRIVILEGED writer may use; Bfree is what is genuinely
	// unoccupied. They differ by the filesystem's reserved blocks, 5% by default
	// on ext4, which is 100 GiB on the 2 TB shard. Deriving used from Bavail
	// counted that reserve as occupied: /health reported 107 GiB used while df
	// said 8.7G, and the watermark would have refused writes ~100 GiB early.
	// free stays Bavail because that IS what we can write into.
	free = uint64(st.Bavail) * bsize
	if blocks := uint64(st.Blocks); blocks >= uint64(st.Bfree) {
		used = (blocks - uint64(st.Bfree)) * bsize
	}
	return used, free, total
}
