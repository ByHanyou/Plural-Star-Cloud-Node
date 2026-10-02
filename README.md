# Plural Star Cloud Node

> **You only need this if you are running your own private Plural Star network.**
>
> The public Plural Star network is already served by Plural Star's own Cloud Node. Every copy of the Plural Star app connects to it out of the box. If you just use the app, or you run a relay on the public network, there is nothing here for you to install, configure, or host. This repository is for people who want a network of their own: a closed group of friends or systems on a node they run themselves, with its own key, that the public network cannot see.

The Cloud Node is the server side of Plural Star's Friend & Syncing System. It is a standalone program, separate from the app, built on [go-libp2p](https://github.com/libp2p/go-libp2p). It relays end-to-end-encrypted packets between Plural Star apps and, when the cloud role is switched on, also holds packets for apps that are offline, stores encrypted vault backups, caches signed front announcements, and relays push registrations. A node sees peer IDs and opaque encrypted blobs. It never sees a password, an encryption key, a message, a member, or a piece of media.

## Related projects & links

<p align="center">
  <a href="https://www.buymeacoffee.com/PluralStar">
    <img src="https://img.buymeacoffee.com/button-api/?text=Support+PS&amp;emoji=%E2%98%95&amp;slug=PluralStar&amp;button_colour=151929&amp;font_colour=ffffff&amp;font_family=Cookie&amp;outline_colour=ffffff&amp;coffee_colour=FFDD00" alt="Support Plural Star on Buy Me a Coffee" />
  </a>
  &nbsp;
  <a href="https://discord.gg/FFQw33cu8m">
    <img src="https://img.shields.io/badge/Discord-Join%20Us-5865F2?style=for-the-badge&logo=discord&logoColor=white" alt="Join our Discord" />
  </a>
</p>

- **Plural Star** (the app this node serves): [Website](https://byhanyou.github.io/Plural-Star/) · [GitHub](https://github.com/ByHanyou/Plural-Star) · [App Store](https://apps.apple.com/in/app/plural-star/id6763964266) · [Google Play](https://play.google.com/store/apps/details?id=com.pluralspace.app)
- **Plural Star Desktop**: [GitHub](https://github.com/ByHanyou/Plural-Star-Desktop) · [Latest release](https://github.com/ByHanyou/Plural-Star-Desktop/releases/latest)
- **PS Legacy** (32-bit Android): [GitHub](https://github.com/ByHanyou/Plural-Star-Legacy)
- **Plural Star Node** (the earlier relay-only node this one replaces): [GitHub](https://github.com/ByHanyou/Plural-Star-Node)
- **Community**: [Reddit](https://www.reddit.com/r/PluralStar/)

## Who runs what

| You are | What you need |
|---|---|
| A Plural Star user on the default network | Nothing. The app already talks to Plural Star's node. |
| Running a relay on the public network | Nothing from this repo. The public network's cloud services come from Plural Star's node only. |
| Running your own private network | This. Build it, create a key, run it in `private` mode, point your apps at it. |

## What it does

- Joins a libp2p mesh in `public`, `private`, or `custom_public` mode.
- Relays encrypted packets between connected Plural Star apps, with packet dedup for multi-path redundancy and an in-memory routing table built from gossiped presence.
- Exposes a local WebSocket + REST API (default `127.0.0.1:7523`) that the app connects to.
- Keeps a pairing directory (`/rendezvous/*`) so two apps can find each other by friend code.
- With `cloud.enabled: true`, advertises roles `[relay, cloud]` and adds the store-and-forward inbox, encrypted vault, signed front cache, and push relay under `/cloud/*`.

## Requirements

- Go 1.25.7 or newer. The module declares `go 1.25.7`, so an older toolchain-aware Go downloads it automatically.

## Build

Linux / macOS:

```sh
make build
./plural-star-cloud-node --config config.yaml
```

Without `make`:

```sh
CGO_ENABLED=0 go build -o plural-star-cloud-node ./cmd/node
```

Windows (`cmd` or PowerShell):

```bat
go build -o plural-star-cloud-node.exe ./cmd/node
plural-star-cloud-node.exe --config config.yaml
```

`make dist` cross-compiles fully static binaries for every release target into `dist/`: linux amd64, linux arm64 (Pi 4/5), linux armv7 (Pi 3), macOS arm64, windows amd64.

On first run with no config the node writes a default `config.yaml` (public mode) and generates an Ed25519 identity in `node.key`. Keep `node.key`: it is the node's peer ID, and the address other nodes and apps use to reach it.

## Running your own private network

A private network is enforced with a libp2p pre-shared key (`pnet`). Nodes without the key cannot complete the handshake, there is no DHT and no public discovery, and the public network never learns the network exists.

**1. Create the key.** It must be exactly 32 bytes.

Linux / macOS:

```sh
head -c 32 /dev/urandom > network.psk
chmod 600 network.psk
```

Windows (PowerShell):

```powershell
$b = New-Object byte[] 32; [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($b); [IO.File]::WriteAllBytes("$PWD\network.psk", $b)
```

**2. Configure the node.** Copy `config.yaml.example` to `config.yaml` and set:

```yaml
network_mode: "private"
psk_path: "./network.psk"
bootstrap_peers:
  - "/ip4/<this-node's-lan-or-public-ip>/tcp/4001/p2p/<this-node's-peer-id>"
api_host: "0.0.0.0"
api_token: "<a long random secret>"
```

`bootstrap_peers` must have at least one entry in private mode. On the first and only node of a network, list the node's own address: it is logged at startup as `node peer ID: 12D3Koo...`, and the self-dial is skipped harmlessly. Later nodes list the first node instead.

`api_host: "0.0.0.0"` lets apps on other machines reach the API. `api_token` gates every endpoint, so set the same token in each app. A token is honoured on `private` and `custom_public` networks only. On a `public` node it is stripped and rewritten to `""` on load, with a log line saying so, because apps on the default network have no token and setting one there locks every one of them out.

**3. Point the apps at it.** In the Plural Star app, open Network Settings and enter the node's URL (for example `http://192.168.1.20:7523`) as the Relay URL, plus the token. Every app on the network does the same.

**4. Add a second node (optional).** On the running node:

```sh
curl -X POST -H "Authorization: Bearer <api_token>" http://127.0.0.1:7523/invite/generate
```

Then on the new node, after its first run has created `config.yaml` and `node.key`:

```sh
curl -X POST -H "Content-Type: application/json" -d '{"invite":"<the invite string>"}' http://127.0.0.1:7523/invite/accept
```

That writes the PSK and bootstrap address into the new node's config. Restart it.

**Private networks run TCP-only.** go-libp2p's QUIC transport does not support private networks, so the QUIC listen address is dropped automatically when a PSK is set. Forward **TCP 4001** between nodes, and **TCP 7523** if apps connect from outside the LAN. Set `announce_addrs` to the node's reachable address if it sits behind NAT.

### Cloud services on a private network

The cloud role is off by default, and on the public network it is provided by Plural Star's node only. On a network of your own, turning it on is one line:

```yaml
cloud:
  enabled: true
  storage_path: "/var/lib/plural-star-cloud-node/cloud"
```

Apps detect cloud support by reading `/health` of the node they are on; nothing else needs configuring on the app side. With it on, the node keeps packets for offline apps on disk (newest packet per sender per recipient, 30 days, 50 MB per recipient by default) instead of in memory, serves encrypted vault backups with per-vault quotas, caches signed front announcements, and relays push registrations to a push gateway if `push_forward_url` is set. Vault data can grow large: on a deployed node put `storage_path` on the data volume. The X25519 key in `push.key` is generated on first start and must be backed up, because a regenerated key orphans anything sealed to the old one.

All `cloud.*` fields are documented in `config.yaml.example`.

## Configuration

See `config.yaml.example`. Key fields:

| Field | Meaning |
|---|---|
| `keypair_path` | Ed25519 identity file, generated on first run if absent |
| `require_existing_identity` | `true` makes a missing keypair a fatal error instead of a silently regenerated identity. Set it once the node's peer ID is known to others |
| `network_mode` | `public`, `private`, or `custom_public` |
| `psk_path` | 32-byte pre-shared key file (required for `private`) |
| `network_id` | namespacing string (required for `custom_public`) |
| `bootstrap_peers` | multiaddr list; required for `private` and `custom_public`, built-in defaults for `public` |
| `directory_url` | optional hosted directory of signed network cards (`public` / `custom_public`) |
| `api_host` | API bind address; `127.0.0.1` (default, local only) or `0.0.0.0` to allow apps to connect over the network |
| `api_port` / `api_token` | API port and bearer token (`private` / `custom_public` only) |
| `listen_addrs` / `announce_addrs` | libp2p listen multiaddrs, and the external address to advertise behind NAT |
| `relay_enabled` | whether this node forwards traffic for others |
| `max_peers` / `max_app_connections` | connection limits |
| `debug_addr` | optional pprof listener; loopback addresses only, refused otherwise |
| `cloud.*` | cloud role, storage path, media backend (`local`, `remote`, `s3`), quotas, inbox retention, GC hour, push forwarding |

Paths: `keypair_path` and `psk_path` resolve against the process working directory; `cloud.storage_path` and `cloud.push_box_key_path` resolve against the directory the config file lives in. Use absolute paths on any deployed node.

### Network modes

- **Public**: open join via Kad-DHT + GossipSub under namespace `plural-star/global`. If no bootstrap peers are set, the built-in defaults are used.
- **Private**: PSK-enforced via libp2p `pnet`. No DHT or public discovery; bootstrap peers must be specified. TCP-only.
- **Custom public**: open join scoped under `plural-star/<network_id>`, separate from the global network.

## API

When `api_token` is set, all endpoints require `Authorization: Bearer <api_token>` (WebSocket clients may instead pass `?token=<api_token>`). On a public node the token is always empty and every endpoint is open.

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Status, peer ID, mode, network ID, connected node/app counts, uptime, `roles`, `agent`, `pressure`, and a `cloud` block when the cloud role is on |
| GET | `/nodes` | Connected libp2p nodes with RTT and roles |
| GET | `/peers` | Known online app peers (from the routing table) |
| GET | `/networks` | Known public networks (from the discovery cache) |
| POST | `/send` | Send a packet: `{recipient_peer_id, payload(base64), packet_id?}` → `{status:"queued", packet_id}` (best-effort). Reuse the returned `packet_id` when sending the same packet through other nodes so duplicates are deduped end-to-end |
| POST | `/invite/generate` | Generate a private-network invite (private mode only) |
| POST | `/invite/accept` | Accept an invite; writes PSK + bootstrap, requires restart |
| GET | `/config` | Current config (API token redacted) |
| POST | `/rendezvous/register` | Publish a signed, short-lived pairing record under a namespace |
| GET | `/rendezvous/lookup` | Look up pairing records: `?namespace=<ns>` |
| GET | `/ws` | WebSocket; pass `?peer_id=<app peer id>` to register |

WebSocket events (node → app), each a JSON object with a `type`: `packet_received`, `peer_online`, `peer_offline`, `node_connected`, `node_disconnected`, `error`.

With the cloud role on, `/cloud/*` is added: `vault/lookup`, `vault/create`, `vault/info`, `vault/manifest`, `vault/objects/{id}`, `vault/devices`, `vault/rewrap`, `inbox`, `front/{peer_id}`, `push/register`, `push/event`, and `blob/*` on a node that serves its blob store to another node.

## Running as a service

### Linux (systemd)

A hardened unit is in `scripts/plural-star-cloud-node.service`. It runs as user `plural-star`, reads `/etc/plural-star-cloud-node/config.yaml`, works in `/var/lib/plural-star-cloud-node`, and sets `GOMEMLIMIT=2500MiB`.

```sh
sudo useradd --system --home /var/lib/plural-star-cloud-node plural-star
sudo install -Dm755 plural-star-cloud-node /usr/local/bin/plural-star-cloud-node
sudo install -Dm644 scripts/plural-star-cloud-node.service /etc/systemd/system/plural-star-cloud-node.service
sudo mkdir -p /etc/plural-star-cloud-node /var/lib/plural-star-cloud-node
sudo chown plural-star:plural-star /var/lib/plural-star-cloud-node
sudo systemctl enable --now plural-star-cloud-node
```

Put `config.yaml`, `node.key`, and `network.psk` in `/etc/plural-star-cloud-node/`, owned by `plural-star`, and use absolute paths in the config.

### Windows

Run it at startup with [NSSM](https://nssm.cc/), from an Administrator `cmd`:

```bat
nssm install PluralStarCloudNode "C:\path\to\plural-star-cloud-node.exe" --config "C:\path\to\config.yaml"
nssm start PluralStarCloudNode
```

Or create a Task Scheduler task that runs `plural-star-cloud-node.exe --config config.yaml` with the trigger "At startup."

## The push gateway (not part of this repo)

iOS friend notifications and Live Activity updates are delivered by a separate, single-instance push gateway operated by Plural Star. Pushes are cryptographically tied to the official app's bundle ID and Plural Star's Apple account key, so one gateway serves every install of the official app, on any network. It is not distributed with this repo and running a node, public or private, never involves it. A Cloud Node only forwards push registrations and events unchanged to the gateway at `cloud.push_forward_url`; with that field empty, `/cloud/push/*` answers 503 and everything else works as normal.

## Architecture

```
cmd/node            entry point, subsystem wiring, signal handling
cmd/gencard         signs network cards for a hosted directory
internal/config     config load/validate + first-run generation
internal/host       go-libp2p host, identity, DHT, peer memory
internal/network    network modes, mDNS + DHT discovery, invites,
                    signed network cards, bbolt cache
internal/relay      packet, dedup cache, routing table, presence gossip,
                    in-memory queue, /plural-star/relay/1.0.0 stream handler
internal/ping       connected-node tracking + RTT (libp2p ping)
internal/api        REST + WebSocket server, bearer auth, rendezvous
internal/cloud      inbox, vault, objects, blob backends (local/remote/s3),
                    front cache, push relay, quotas, GC
```

## Security model

Relay nodes are untrusted. They route on `RecipientID` and never inspect `Payload`. End-to-end encryption is the app's responsibility; the node treats payloads as opaque bytes. Node-to-node transport is encrypted by libp2p (Noise). The cloud role stores ciphertext and index data only: it sees vault IDs, object hashes and sizes, upload and download timing, and peer IDs for the inbox, front cache, and push relay. A forgotten vault password makes that vault unrecoverable by anyone, including the operator. The API binds to `127.0.0.1` by default; if you expose it beyond your LAN you are responsible for TLS via a reverse proxy.

## Privacy & data

The node stores no personal data. It handles libp2p peer IDs of connected apps (kept in memory, never written to disk; peer IDs of other nodes are cached on disk with their public addresses so the mesh can redial itself after an outage) and encrypted payloads (passed through opaquely, never inspected or logged). The files a node writes are operational: its own keypair (`node.key`), config (`config.yaml`), an optional private-network PSK (`network.psk`), a cache of public network cards (`networks.db`), a cache of recently connected node peers (`known_peers.json`), and persisted pairing-directory records (`rendezvous.json`). With the cloud role on it also writes, under `cloud.storage_path`, the encrypted inbox packets, vault manifests and objects, signed front records, queued push events, and the push box key (`push.key`).

Because the node neither collects nor stores personal data, it does not by itself require a formal privacy policy. The operator of a private network may still want to tell their members what the node logs and keeps. A full privacy policy belongs with the Plural Star app, which handles user content.

## License

[AGPL-3.0-or-later](LICENSE). Third-party notices are in `THIRD-PARTY-NOTICES.md`.
