// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	lpcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
)

func testIdentity(t *testing.T) (peer.ID, ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	lp, err := lpcrypto.UnmarshalEd25519PublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := peer.IDFromPublicKey(lp)
	if err != nil {
		t.Fatal(err)
	}
	return pid, pub, priv
}

const testSkew = 10 * time.Minute

func TestReplayCacheSeenOrAdd(t *testing.T) {
	c := &replayCache{seen: make(map[string]time.Time)}
	if c.seenOrAdd("sig-1", testSkew) {
		t.Fatal("first use reported as replay")
	}
	if !c.seenOrAdd("sig-1", testSkew) {
		t.Fatal("second use not reported as replay")
	}
	c.seen["stale"] = time.Now().Add(-3 * testSkew)
	if c.seenOrAdd("stale", testSkew) {
		t.Fatal("a signature older than the skew window still counts as a replay")
	}
}

func TestHashSecret(t *testing.T) {
	hexSecret := strings.Repeat("ab", 32)
	raw, _ := hex.DecodeString(hexSecret)
	sum := sha256.Sum256(raw)
	if hashSecret(hexSecret) != hex.EncodeToString(sum[:]) {
		t.Fatal("hex secret is not hashed from its bytes")
	}
	if !secretMatches(hashSecret(hexSecret), hexSecret) {
		t.Fatal("matching secret refused")
	}
	if secretMatches(hashSecret(hexSecret), strings.Repeat("cd", 32)) {
		t.Fatal("wrong secret accepted")
	}
	rawSum := sha256.Sum256([]byte("not-hex"))
	if hashSecret("not-hex") != hex.EncodeToString(rawSum[:]) {
		t.Fatal("non-hex secret is not hashed from its text")
	}
}

func TestVerifySignedAndIdentityHeaders(t *testing.T) {
	pid, pub, priv := testIdentity(t)
	pubB64 := base64.StdEncoding.EncodeToString(pub)
	signed := "pscloud-test|" + pid.String()
	sigB64 := base64.StdEncoding.EncodeToString(ed25519.Sign(priv, []byte(signed)))
	if !verifySigned(pid.String(), pubB64, sigB64, signed) {
		t.Fatal("valid signature refused")
	}
	if verifySigned(pid.String(), pubB64, sigB64, signed+"x") {
		t.Fatal("tampered message accepted")
	}
	if verifySigned(test.RandPeerIDFatal(t).String(), pubB64, sigB64, signed) {
		t.Fatal("signature accepted for a peer id that does not match the key")
	}
	if verifySigned(pid.String(), "bad", sigB64, signed) || verifySigned(pid.String(), pubB64, "bad", signed) {
		t.Fatal("malformed key or signature accepted")
	}

	ts := time.Now().UnixMilli()
	msg := fmt.Sprintf("pscloud-inbox|%s|%d", pid, ts)
	sig := base64.StdEncoding.EncodeToString(ed25519.Sign(priv, []byte(msg)))
	req := httptest.NewRequest("GET", "/cloud/inbox", nil)
	req.Header.Set(headerPeer, pid.String())
	req.Header.Set(headerPub, pubB64)
	req.Header.Set(headerSig, sig)
	req.Header.Set(headerTS, strconv.FormatInt(ts, 10))
	h, ok := readIdentityHeaders(req)
	if !ok {
		t.Fatal("identity headers not read")
	}
	if !h.verify(msg, testSkew) {
		t.Fatal("fresh signed headers refused")
	}
	if h.verify(msg, testSkew) {
		t.Fatal("replayed signature accepted")
	}
	if freshTS(time.Now().Add(-2*testSkew).UnixMilli(), testSkew) {
		t.Fatal("stale timestamp counted as fresh")
	}
	req.Header.Del(headerSig)
	if _, ok := readIdentityHeaders(req); ok {
		t.Fatal("headers without a signature accepted")
	}
}
