// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"time"

	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p/core/crypto"
)

const wsAuthDomain = "plural-star:relay-auth:v1:"

type wsAuthMessage struct {
	Type      string `json:"type"`
	Nonce     string `json:"nonce,omitempty"`
	Signature string `json:"signature,omitempty"`
	Session   string `json:"session,omitempty"`
}

func authenticateWS(conn *websocket.Conn, key crypto.PubKey, writeWait time.Duration) (string, error) {
	nonce := hex.EncodeToString(randBytes())
	conn.SetReadLimit(1024)
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return "", err
	}
	if err := conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
		return "", err
	}
	if err := conn.WriteJSON(wsAuthMessage{Type: "auth_challenge", Nonce: nonce}); err != nil {
		return "", err
	}
	var response wsAuthMessage
	if err := conn.ReadJSON(&response); err != nil {
		return "", err
	}
	sig, err := base64.StdEncoding.DecodeString(response.Signature)
	if err != nil || response.Type != "auth_response" {
		return "", errors.New("invalid websocket authentication response")
	}
	if ok, err := key.Verify([]byte(wsAuthDomain+nonce), sig); !ok || err != nil {
		return "", errors.New("invalid websocket authentication signature")
	}
	return hex.EncodeToString(randBytes()), nil
}

func randBytes() []byte {
	var b [32]byte
	rand.Read(b[:])
	return b[:]
}
