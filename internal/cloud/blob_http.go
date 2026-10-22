// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
)

func blobGetResult(resp *http.Response, label, id string) (io.ReadCloser, int64, error) {
	if resp.StatusCode == http.StatusNotFound {
		_ = resp.Body.Close()
		return nil, 0, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, 0, fmt.Errorf("%s get %s: status %d", label, id, resp.StatusCode)
	}
	return resp.Body, resp.ContentLength, nil
}

func blobHeadResult(resp *http.Response, label, id string) (int64, error) {
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return 0, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("%s head %s: status %d", label, id, resp.StatusCode)
	}
	size, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	return size, nil
}

func blobDeleteResult(resp *http.Response, label, id string) error {
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("%s delete %s: status %d", label, id, resp.StatusCode)
	}
	return nil
}
