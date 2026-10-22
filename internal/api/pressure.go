// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"os"
	"runtime"
	"strconv"
	"strings"
)

func pressure() map[string]any {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	out := map[string]any{
		"heap_bytes": ms.HeapAlloc,
		"sys_bytes":  ms.Sys,
		"goroutines": runtime.NumGoroutine(),
		"cpus":       runtime.NumCPU(),
	}
	if raw, err := os.ReadFile("/proc/loadavg"); err == nil {
		fields := strings.Fields(string(raw))
		if len(fields) >= 3 {
			for i, name := range []string{"load1", "load5", "load15"} {
				if v, e := strconv.ParseFloat(fields[i], 64); e == nil {
					out[name] = v
				}
			}
		}
	}
	if limit := os.Getenv("GOMEMLIMIT"); limit != "" {
		out["gomemlimit"] = limit
	}
	return out
}
