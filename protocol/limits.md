# Limits, timeouts and signed strings

Numbers the node enforces and the numbers the clients assume, side by side,
each with the line that defines it. When one side changes, change the other or
document why they differ. Paths under `Plural-Star/` are the Mobile app; the
same files are in Legacy, Desktop and Starbot where they exist, byte-identical
apart from the imports and logging calls of Starbot's `cloudVault.ts`. A node
value written as `limits.<key>` is set in the `limits:` block of the node's
config; the number beside it is the default.

## Cloud vault (`/cloud/vault/*`)

| Limit | Node | Client | Where |
|---|---|---|---|
| JSON request body | `limits.max_json_body_bytes`, default 64 KiB | | `internal/config/limits.go`; `internal/cloud/vault_handlers.go:15`, `:35`, `:150`, `device_handlers.go:30`, `:89`, `front_handlers.go:69` |
| Manifest body (`PUT vault/manifest`) | `limits.max_manifest_body_bytes`, default 32 MiB | | `internal/config/limits.go`; `internal/cloud/vault_handlers.go:125` |
| One object | `object_max_mb`, default 25 MiB | 25 MiB, larger keys are left out before upload | `internal/config/cloud.go:60`; `internal/cloud/objects.go` (`ErrTooLarge`); `Plural-Star/src/cloud/cloudVault.ts:48` (`OBJECT_MAX_BYTES`) |
| Upload chunk | any, resumed by `Content-Range` | 2 MiB per `PUT` | `Plural-Star/src/cloud/cloudApi.ts:12` (`UPLOAD_CHUNK_BYTES`), `putChunk` |
| Base tier quota | `base_quota_mb`, default 200 MiB | | `internal/config/cloud.go:58` |
| Media tier quota | `media_quota_mb`, default 1024 MiB | | `internal/config/cloud.go:59` |
| Disk watermark | `watermark_percent`, default 90, allowed 50–99; writes refused above it | | `internal/config/cloud.go:57`, `:92`; `handlers.go` (`watermarked`) |
| Lookup / bad-secret rate limit | `limits.rate_limit_per_address_per_minute`, default 10; `limits.rate_limit_per_vault_per_hour`, default 20; counted on `VAULT_MISSING` and `BAD_AUTH` | honours `Retry-After` / `retry_after_seconds` | `internal/config/limits.go`; `internal/cloud/ratelimit.go:12-13` (windows); `cloudApi.ts` (`fail`), `cloudVault.ts` (`scheduleRetry`) |
| JSON call timeout | | 15 s, `config.cloud.jsonTimeoutMs` | `Plural-Star/src/cloud/cloudTypes.ts:272` (`DEFAULT_CLOUD_LIMITS`) |
| Object call timeout | | 120 s, `config.cloud.objectTimeoutMs` | `Plural-Star/src/cloud/cloudTypes.ts:273` |
| Statuses the client retries later instead of refusing the key | | 401 403 404 408 409 423 425 429 | `Plural-Star/src/cloud/cloudVault.ts:52` (`RETRY_STATUSES`) |
| Retry backoff | | 5 s base, 5 min cap (`config.cloud.retryBaseMs`, `retryMaxMs`), never shorter than `Retry-After` | `Plural-Star/src/cloud/cloudTypes.ts:275-276`; `cloudVault.ts` (`scheduleRetry`) |
| Push debounce after a local change | | 4 s, `config.cloud.pushDebounceMs` | `Plural-Star/src/cloud/cloudTypes.ts:274` |
| Minimum gap between checks | | 20 s, `config.cloud.checkMinIntervalMs` | `Plural-Star/src/cloud/cloudTypes.ts:277` |
| Manifest conflict retries | | 3 rounds, then `MANIFEST_VERSION_CONFLICT` | `Plural-Star/src/cloud/cloudVault.ts` (`syncOnce`) |

Vault credentials travel in headers `X-Vault-Id` and `X-Vault-Auth` (64 hex
characters each) or, for `lookup` and `create`, in the JSON body
(`internal/cloud/auth.go:25-26`, `handlers.go` `creds`). Device identity uses
`X-Ps-Device-Id` and `X-Ps-Device-Key` (`auth.go:32-33`). Object uploads carry
`X-Ps-Tier: base|media` (`auth.go:31`), `Content-Range: bytes a-b/total`, and
read back `Upload-Offset` on a resumable conflict (`auth.go:34`).

## Fronts, inbox and push (`/cloud/front`, `/cloud/inbox`, `/cloud/push/*`)

| Limit | Node | Client | Where |
|---|---|---|---|
| Signed timestamp skew | `limits.auth_timestamp_skew`, default ±10 min; replay cache keeps signatures for twice that | shares with `at <= now + 5 min` | `internal/config/limits.go`; `internal/cloud/auth.go:63`, `:115`, `:120`; `Plural-Star/src/network/inbound.ts:94` (`FRONT_CLOCK_SKEW_MS`) |
| `fronters` string | 400 runes | | `internal/cloud/front_handlers.go:16` (`maxFrontersLen`) |
| `name` | 64 runes | | `internal/cloud/front_handlers.go:17` (`maxNameLen`) |
| `readers` | 500, extra entries dropped | sends up to 500 peers in a batch read | `internal/cloud/front_handlers.go:18` (`maxReaders`), `:82`; `Plural-Star/src/network/gateway.ts:81`, `:115` (`.slice(0, 500)`) |
| Push body (`push/register`, `push/event`) | `limits.push_max_body_bytes`, default 16 KiB | | `internal/config/limits.go`; `internal/cloud/push_handlers.go:23` (`pushRequest`) |
| Push relay | 503 `PUSH_DISABLED` when `cloud.push_forward_url` is empty | | `internal/cloud/push.go:35` (`ErrPushDisabled`); `push_handlers.go:20` |

Header-signed routes (`GET /cloud/inbox`, `GET /cloud/front/{peer_id}`) read
`X-Ps-Peer`, `X-Ps-Pub`, `X-Ps-Sig`, `X-Ps-Ts` (`auth.go:27-30`). Body-signed
routes (`PUT /cloud/front/{peer_id}`, `POST /cloud/push/*`) carry `peer_id`,
`ed_pub`, `sig`, `ts` in the JSON.

## Node API (`/send`, `/ws`, rendezvous, relay)

| Limit | Node | Client | Where |
|---|---|---|---|
| `/send` body | `limits.max_send_body_bytes`, default 1 MiB | | `internal/config/limits.go`; `internal/api/handlers.go:128` |
| `/invite/accept` body | `limits.max_invite_body_bytes`, default 64 KiB | | `internal/config/limits.go`; `internal/api/handlers.go:321` |
| `/rendezvous/register` body | `limits.max_rendezvous_body_bytes`, default 64 KiB | | `internal/config/limits.go`; `internal/api/handlers.go:359` |
| WebSocket message | `limits.ws_max_message_bytes`, default 64 KiB | sync messages are budgeted to 64 KiB | `internal/config/limits.go`; `internal/api/wsclient.go:93`; `Plural-Star/src/sync/transfer.ts:6` (`SYNC_MSG_BUDGET`) |
| Relay packet | `limits.max_packet_bytes`, default 2 MiB | | `internal/config/limits.go`; `internal/relay/manager.go:259` |
| Relay stream open and write | `limits.relay_stream_timeout`, default 15 s | | `internal/config/limits.go`; `internal/relay/manager.go:242` |
| Rendezvous record | `limits.rendezvous_max_record_bytes`, default 8 KiB | | `internal/config/limits.go`; `internal/api/handlers.go:370` |
| Rendezvous namespace | `limits.rendezvous_max_namespace_len`, default 128 bytes | | `internal/config/limits.go`; `internal/api/handlers.go:366`; `internal/relay/rendezvous.go` (gossip receiver) |
| Rendezvous TTL | `limits.rendezvous_max_ttl`, default 3600 s cap | asks for 1800 s | `internal/config/limits.go`; `internal/api/handlers.go:374`; `Plural-Star/src/network/types.ts:260` (`RENDEZVOUS_TTL_SECONDS`) |
| Rendezvous entries | `limits.rendezvous_max_entries`, default 20000, then 503 | | `internal/config/limits.go`; `internal/api/rendezvous.go:141`, `:158` |

## App-to-app sync and mirror (over the relay, client only)

| Limit | Value | Where |
|---|---|---|
| Protocol version | 2 | `Plural-Star/src/network/types.ts:34` (`PROTO_VERSION`) |
| Sync message budget | 64 KiB | `Plural-Star/src/sync/transfer.ts:6` (`SYNC_MSG_BUDGET`) |
| Sync chunk | 48 KiB | `transfer.ts:7` (`SYNC_CHUNK_SIZE`) |
| Sync parts per transfer | 4096 | `transfer.ts:8` (`SYNC_MAX_PARTS`) |
| Mirror media item | 600 KiB | `Plural-Star/src/network/types.ts:177` (`MIRROR_MEDIA_MAX`), used by `mirror/serve.ts` and `mirror/request.ts` |
| Mirror media requests outstanding | 400 | `types.ts:178` (`MIRROR_MEDIA_REQ_MAX`) |
| Inbound sanitiser | depth 12, 20000 items, 400 keys per object, 400000 chars per string | `Plural-Star/src/network/inbound.ts:64-67` |
| Mirror GIF | 8 MiB, 384 KiB parts, 96 KiB minimum part, 200 asks | `types.ts:171-176` |
| Friend tombstones | 90 days, 200 kept | `types.ts:60-61` |
| Friends with notifications | 5 | `types.ts:262` (`MAX_NOTIF_FRIENDS`) |

## Signed strings

Ed25519 detached signatures over the UTF-8 bytes of the string, base64 in
`sig`; `ed_pub` is the signer's public key, `peer_id` must derive from it. The
node verifies every form listed for a route and accepts the first that matches.

| Purpose | String | Sent by | Verified at |
|---|---|---|---|
| Front publish, current | `psgw-front\|peer\|ts\|fronters\|startTime\|name\|primary\|coFront\|coConscious\|readers` (readers comma-joined) | `Plural-Star/src/network/gateway.ts:86` → gateway `/gw/front`; same body accepted by `PUT /cloud/front/{peer_id}` and `POST /cloud/push/event` | `internal/cloud/front_handlers.go:44`, `push_handlers.go:93` |
| Front publish, previous | `psgw-front\|peer\|ts\|fronters\|startTime\|name` | `gateway.ts:92` (fallback when the gateway rejects the current form) | `front_handlers.go:47`, `push_handlers.go:93` |
| Front publish, legacy | `psgw-front\|peer\|ts\|fronters\|startTime` | no longer sent | `front_handlers.go:48`, `push_handlers.go:93` |
| Fronts batch read | `psgw-fronts\|peer\|ts\|peers` (comma-joined, up to 500) | `gateway.ts:118` → gateway `/gw/fronts` | push gateway only (not in this repo) |
| Push register, current | `psgw-register\|peer\|ts\|env\|activityToken\|deviceToken\|watch\|pinned` (lists comma-joined) | `gateway.ts:200` → gateway `/gw/register`; same body accepted by `POST /cloud/push/register` | `push_handlers.go:74` |
| Push register, previous | `psgw-register\|peer\|ts\|env\|activityToken\|watch` | no longer sent | `push_handlers.go:77` |
| Inbox read | `pscloud-inbox\|peer\|ts` | no app in this family calls `GET /cloud/inbox` today | `inbox_handlers.go:14` |
| Front read | `pscloud-front-get\|peer\|ts\|target` | no app in this family calls `GET /cloud/front/{peer_id}` today | `front_handlers.go:109` |

`ts` is Unix milliseconds and must be within the skew above. A rejected
signature is `401 BAD_SIGNATURE`; a stale `ts` is `400 STALE_TIMESTAMP`.
