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
	return freshTS(h.TS) && verifySigned(h.PeerID, h.Pub, h.Sig, signed)
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
		return r.RemoteAddr
	}
	return host
}
