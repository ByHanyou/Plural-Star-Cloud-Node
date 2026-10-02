// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build windows

package cloud

import "golang.org/x/sys/windows"

func diskUsage(path string) (used, free, total uint64) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, 0, 0
	}
	var freeAvail, totalBytes, totalFree uint64
	if err := windows.GetDiskFreeSpaceEx(p, &freeAvail, &totalBytes, &totalFree); err != nil {
		return 0, 0, 0
	}
	total = totalBytes
	free = freeAvail
	if total >= totalFree {
		used = total - totalFree
	}
	return used, free, total
}
