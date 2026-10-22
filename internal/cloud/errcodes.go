// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"net/http"
	"strconv"
)

const (
	CodeBadRequest          = "BAD_REQUEST"
	CodeUnauthorized        = "UNAUTHORIZED"
	CodeForbidden           = "FORBIDDEN"
	CodeNotFound            = "NOT_FOUND"
	CodeMethodNotAllowed    = "METHOD_NOT_ALLOWED"
	CodeConflict            = "CONFLICT"
	CodeTooLarge            = "TOO_LARGE"
	CodeUnsupportedMedia    = "UNSUPPORTED_MEDIA_TYPE"
	CodeRateLimited         = "RATE_LIMITED"
	CodeInternal            = "INTERNAL"
	CodeUnavailable         = "UNAVAILABLE"
	CodeInsufficientStorage = "INSUFFICIENT_STORAGE"

	CodeVaultMissing            = "VAULT_MISSING"
	CodeBadAuth                 = "BAD_AUTH"
	CodeDeviceKeyRequired       = "DEVICE_KEY_REQUIRED"
	CodeDeviceUnlinked          = "DEVICE_UNLINKED"
	CodeDeviceKeyIssued         = "DEVICE_KEY_ISSUED"
	CodeVaultExists             = "VAULT_EXISTS"
	CodeManifestVersionConflict = "MANIFEST_VERSION_CONFLICT"
	CodeManifestMissingObjects  = "MANIFEST_MISSING_OBJECTS"
	CodeQuotaExceeded           = "QUOTA_EXCEEDED"
	CodeObjectTooLarge          = "OBJECT_TOO_LARGE"
	CodeHashMismatch            = "HASH_MISMATCH"
	CodeUploadOffsetMismatch    = "UPLOAD_OFFSET_MISMATCH"
	CodeDiskAtWatermark         = "DISK_AT_WATERMARK"
	CodeBadSignature            = "BAD_SIGNATURE"
	CodeStaleTimestamp          = "STALE_TIMESTAMP"
	CodePushDisabled            = "PUSH_DISABLED"
)

func CodeForStatus(status int) string {
	switch status {
	case http.StatusBadRequest:
		return CodeBadRequest
	case http.StatusUnauthorized:
		return CodeUnauthorized
	case http.StatusForbidden:
		return CodeForbidden
	case http.StatusNotFound:
		return CodeNotFound
	case http.StatusMethodNotAllowed:
		return CodeMethodNotAllowed
	case http.StatusConflict:
		return CodeConflict
	case http.StatusRequestEntityTooLarge:
		return CodeTooLarge
	case http.StatusUnsupportedMediaType:
		return CodeUnsupportedMedia
	case http.StatusTooManyRequests:
		return CodeRateLimited
	case http.StatusInternalServerError:
		return CodeInternal
	case http.StatusServiceUnavailable:
		return CodeUnavailable
	case http.StatusInsufficientStorage:
		return CodeInsufficientStorage
	default:
		return "HTTP_" + strconv.Itoa(status)
	}
}
