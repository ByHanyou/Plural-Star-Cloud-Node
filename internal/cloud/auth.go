// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	lpcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	tsSkew           = 10 * time.Minute
	headerVaultID    = "X-Vault-Id"
	headerVaultAuth  = "X-Vault-Auth"
	headerPeer       = "X-Ps-Peer"
	headerPub        = "X-Ps-Pub"
	headerSig        = "X-Ps-Sig"
	headerTS         = "X-Ps-Ts"
	headerTier       = "X-Ps-Tier"
	headerUploadOffs = "Upload-Offset"
)

var hex32 = regexp.MustCompile(`^[0-9a-f]{64}$`)

func validHex32(s string) bool { return hex32.MatchString(s) }

func verifySigned(peerID, edPubB64, sigB64, signed string) bool {
	pub, err := base64.StdEncoding.DecodeString(edPubB64)
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return false
	}
	lp, err := lpcrypto.UnmarshalEd25519PublicKey(pub)
	if err != nil {
		return false
	}
	derived, err := peer.IDFromPublicKey(lp)
	if err != nil || derived.String() != peerID {
		return false
	}
	sig, err := base64.StdEncoding.DecodeString(sigB64)
	if err != nil {
		return false
	}
	return ed25519.Verify(ed25519.PublicKey(pub), []byte(signed), sig)
}

func freshTS(tsMillis int64) bool {
	d := time.Since(time.UnixMilli(tsMillis))
	if d < 0 {
		d = -d
	}
	return d <= tsSkew
}

type identityHeaders struct {
	PeerID string
	Pub    string
	Sig    string
	TS     int64
}

func readIdentityHeaders(r *http.Request) (identityHeaders, bool) {
	h := identityHeaders{
		PeerID: strings.TrimSpace(r.Header.Get(headerPeer)),
		Pub:    strings.TrimSpace(r.Header.Get(headerPub)),
		Sig:    strings.TrimSpace(r.Header.Get(headerSig)),
	}
	ts, err := strconv.ParseInt(strings.TrimSpace(r.Header.Get(headerTS)), 10, 64)
	if err != nil || h.PeerID == "" || h.Pub == "" || h.Sig == "" {
		return h, false
	}
	h.TS = ts
	if _, err := peer.Decode(h.PeerID); err != nil {
		return h, false
	}
	return h, true
}

func (h identityHeaders) verify(signed string) bool {
	return freshTS(h.TS) && verifySigned(h.PeerID, h.Pub, h.Sig, signed) && !replays.seenOrAdd(h.Sig)
}

// replayCache remembers accepted signatures for the freshness window. Without
// it a captured /cloud/inbox request could be replayed for ten minutes and
// drain the victim's packets onto the attacker's socket.
type replayCache struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

var replays = &replayCache{seen: make(map[string]time.Time)}

func (c *replayCache) seenOrAdd(sig string) bool {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.seen) > 4096 {
		for k, t := range c.seen {
			if now.Sub(t) > 2*tsSkew {
				delete(c.seen, k)
			}
		}
	}
	if t, ok := c.seen[sig]; ok && now.Sub(t) <= 2*tsSkew {
		return true
	}
	c.seen[sig] = now
	return false
}

func hashSecret(secretHex string) string {
	raw, err := hex.DecodeString(secretHex)
	if err != nil {
		raw = []byte(secretHex)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func secretMatches(storedHash, secretHex string) bool {
	got := hashSecret(secretHex)
	return subtle.ConstantTimeCompare([]byte(got), []byte(storedHash)) == 1
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	// Behind a reverse proxy on the same machine every request arrives from
	// loopback; the real address is the first X-Forwarded-For hop. Only a
	// loopback peer is trusted to set it, so a remote client cannot pick its own.
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			if first := strings.TrimSpace(strings.Split(xff, ",")[0]); first != "" {
				return first
			}
		}
	}
	return host
}
