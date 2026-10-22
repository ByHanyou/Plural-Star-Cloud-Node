# Error responses

Every error the node returns is a JSON object:

```json
{"error": "<human text>", "code": "<MACHINE_CODE>", "...": "optional extra fields"}
```

`code` is the contract. Clients branch on it and never on the text. `error` is
for logs and people and its wording may change without notice. The HTTP status
still matters (the clients' retry policy keys on it), but two different
conditions can share a status; `code` tells them apart.

Nodes built before `code` existed send `error` only. The shared client engine
(`cloudTypes.ts`, `cloudErrorKindOf`) maps those bodies in this order:

1. `code` from the body, when it is a known kind.
2. The legacy `error` text, through the table below.
3. A 404 or 405 with no JSON body at all → `ROUTE_MISSING` (an older node that
   lacks the route).
4. The HTTP status (table of generic codes).
5. Any other 5xx → `UNAVAILABLE`; anything else → `UNKNOWN`.

Clients older than the `code` field ignore it and keep reading `error`, so
adding `code` broke nothing.

The Go source of truth is `internal/cloud/errcodes.go`. The TypeScript mirror is
`CLOUD_ERROR_KINDS` in `src/cloud/cloudTypes.ts` (byte-identical in Mobile,
Legacy, Desktop and Starbot). Add a code in both or in neither.

## Generic codes

Sent by `writeError` (both the `/cloud/*` handlers and the node API) when the
handler has nothing more specific to say. `CodeForStatus` picks them from the
status; a status outside this table yields `HTTP_<status>`.

| code | status | typical cause |
|---|---|---|
| `BAD_REQUEST` | 400 | malformed JSON, bad ids, missing or invalid fields |
| `UNAUTHORIZED` | 401 | missing or wrong `api_token`, wrong blob-serve secret |
| `FORBIDDEN` | 403 | cross-site request, operator-only endpoint from a remote caller |
| `NOT_FOUND` | 404 | object, front or rendezvous record missing |
| `METHOD_NOT_ALLOWED` | 405 | wrong verb on a node API route |
| `CONFLICT` | 409 | generic conflict |
| `TOO_LARGE` | 413 | request body over the route's `limits.*_body_bytes` cap |
| `UNSUPPORTED_MEDIA_TYPE` | 415 | `/invite/accept` without `application/json` |
| `RATE_LIMITED` | 429 | generic rate limit |
| `INTERNAL` | 500 | unexpected server failure; `error` carries the Go error text |
| `UNAVAILABLE` | 503 | `max_app_connections` reached, rendezvous store full |
| `INSUFFICIENT_STORAGE` | 507 | generic storage refusal |

## Specific codes (`/cloud/*`)

| code | status | legacy `error` text | meaning | extra fields | who branches on it |
|---|---|---|---|---|---|
| `VAULT_MISSING` | 404 | `no vault` | the lookup id is unknown; counted against the lookup rate limit | | `cloudApi.getManifest` (a 404 that is *not* this means "no manifest yet"); Starbot `isDeadLink` |
| `BAD_AUTH` | 401 | `bad auth secret` | auth secret does not match; counted against the rate limit | | Starbot `isDeadLink`; views show the bad-password text |
| `DEVICE_KEY_REQUIRED` | 401 | `device key required` | the device is linked but sent no (or a wrong) device key | | `cloudVault.recoverDeviceKey` re-issues the key |
| `DEVICE_UNLINKED` | 401 | `device unlinked` | this device was removed from the vault | | `cloudVault.isDeviceGone` drops the local link; Starbot `isRemovedDevice` |
| `DEVICE_KEY_ISSUED` | 409 | `device key already issued` | a key was already issued for this device sub-id | | `cloudVault.isKeyIssued` |
| `VAULT_EXISTS` | 409 | `vault already exists` | `vault/create` on a lookup id that exists | | |
| `MANIFEST_VERSION_CONFLICT` | 409 | `manifest version conflict` | `PUT vault/manifest` against a stale base version | `version` (current) | `cloudVault.syncOnce` re-reads and retries (by status); after three rounds it raises the same kind itself |
| `MANIFEST_MISSING_OBJECTS` | 400 | `manifest references missing objects` | the manifest names object ids the node does not hold | `missing` (ids) | `cloudVault.syncOnce` re-uploads or drops those entries (by status + `missing`) |
| `QUOTA_EXCEEDED` | 413 | `vault quota exceeded` | the tier quota would be exceeded | `base_quota_bytes`, `media_quota_bytes` | `cloudVault.uploadKey` lists the key under *over quota* |
| `OBJECT_TOO_LARGE` | 413 | `object too large` | one object over `object_max_mb` | `max_bytes` | `cloudVault.uploadKey` lists the key under *too large*; views |
| `HASH_MISMATCH` | 400 | `object id does not match ciphertext hash` | uploaded bytes do not hash to the object id | | views show the corrupt-vault text |
| `UPLOAD_OFFSET_MISMATCH` | 409 | `upload offset mismatch` | chunked `PUT` with a `Content-Range` that does not continue the stored prefix | `received`; header `Upload-Offset` | `cloudApi.putChunk` resumes from `received` |
| `DISK_AT_WATERMARK` | 507 | `disk at watermark` | node disk usage is above `watermark_percent`; writes refused | `watermark` | views show the out-of-space text |
| `RATE_LIMITED` | 429 | `rate limited` | too many lookups or bad secrets from this address or for this vault | `retry_after_seconds`; header `Retry-After` | `cloudApi.fail` sets `retryAfterSeconds`; `cloudVault.scheduleRetry` waits at least that long; Starbot `/link` tells the user to wait |
| `BAD_SIGNATURE` | 401 | `bad signature` | the Ed25519 signature over the canonical string did not verify (see `limits.md`) | | |
| `STALE_TIMESTAMP` | 400 | `stale timestamp` | `ts` is more than the allowed skew from node time | | |
| `PUSH_DISABLED` | 503 | `push relay not configured` | the node has no `cloud.push_forward_url` | | |
| `NOT_FOUND` | 404 | `not found` | object, front or blob missing | | `cloudVault.pullEntries` records the entry as unreadable instead of failing the sync |

## Kinds raised by the clients themselves

These never come from the node. The engine raises them with status `0`; they
exist so views and Starbot can branch without reading message text.

| kind | raised by | meaning |
|---|---|---|
| `ROUTE_MISSING` | `cloudErrorKindOf` | 404/405 with no JSON body: the node predates the route (used for the device-key routes) |
| `NOT_LINKED` | `cloudVault`, Starbot `session` | no vault is linked on this device / account |
| `ALREADY_LINKED` | `cloudVault`, Starbot `session` | a vault is already linked |
| `LINK_BROKEN` | Starbot `session` | the vault was deleted or its password changed; the user must `/unlink` and `/link` |
| `LINK_IN_PROGRESS` | Starbot `session` | a link is already running |
| `NOT_LOADED` | Starbot `session` | the vault has not been read from the cloud yet |
| `NO_DATA` | Starbot `session` | the vault has no data yet |
| `NO_MASTER_KEY` | `cloudVault` | the master key is not in memory |
| `NO_PENDING_CREDENTIALS` | `cloudVault` | import or clear called without a pending link |
| `BAD_PASSWORD` | `cloudVault` | the vault exists but the password does not open it |
| `VAULT_EMPTY` | `cloudVault` | the existing vault is empty and the caller did not allow that |
| `VAULT_NOT_CLEARABLE` | `cloudVault` | clear-and-upload is not allowed for this vault |
| `RUN_STOPPED` | `cloudVault` | a newer run superseded this one |
| `MANIFEST_UNDECRYPTABLE` | `cloudVault` | the manifest did not decrypt with this key |
| `MANIFEST_MALFORMED` | `cloudVault` | the manifest decrypted but is not a manifest |
| `OBJECT_HASH_MISMATCH` | `cloudVault` | a downloaded object did not hash to its id |
| `OBJECT_UNDECRYPTABLE` | `cloudVault` | a downloaded object did not decrypt |
| `UNKNOWN` | `cloudErrorKindOf` | nothing above matched |

## Where the clients read these

* `CloudError.kind` on every thrown engine error (`cloudTypes.ts`).
* `CloudStatus.lastErrorKind` for the last recorded sync error, next to the
  display code `lastErrorCode` (`CLOUD-<status>` or `CLOUD-APP`) and the raw
  `lastError` text.
* `CloudStatus.leftOut` for keys a sync left out (too large, over quota,
  refused, unreadable). It is structured so views do not parse `lastError`.
