// SPDX-License-Identifier: AGPL-3.0-or-later

package network

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

const DiscoveryTopic = "plural-star:discovery:networks"

type NetworkCard struct {
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	Description    string   `json:"description"`
	BootstrapPeers []string `json:"bootstrap_peers"`
	NodeCountHint  int      `json:"node_count_hint"`
	CreatedBy      string   `json:"created_by"`
	CreatedAt      int64    `json:"created_at"`
	Signature      string   `json:"signature"`
}

func (c NetworkCard) signingBytes() ([]byte, error) {
	c.Signature = ""
	return json.Marshal(c)
}

func SignNetworkCard(c *NetworkCard, priv crypto.PrivKey) error {
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return fmt.Errorf("derive peer id: %w", err)
	}
	c.CreatedBy = pid.String()
	b, err := c.signingBytes()
	if err != nil {
		return err
	}
	sig, err := priv.Sign(b)
	if err != nil {
		return fmt.Errorf("sign card: %w", err)
	}
	c.Signature = base64.StdEncoding.EncodeToString(sig)
	return nil
}

func VerifyNetworkCard(c *NetworkCard) error {
	if c.Signature == "" {
		return errors.New("card has no signature")
	}
	pid, err := peer.Decode(c.CreatedBy)
	if err != nil {
		return fmt.Errorf("invalid created_by: %w", err)
	}
	pub, err := pid.ExtractPublicKey()
	if err != nil || pub == nil {
		return fmt.Errorf("cannot extract public key from created_by peer id: %w", err)
	}
	sig, err := base64.StdEncoding.DecodeString(c.Signature)
	if err != nil {
		return fmt.Errorf("invalid signature encoding: %w", err)
	}
	b, err := c.signingBytes()
	if err != nil {
		return err
	}
	ok, err := pub.Verify(b, sig)
	if err != nil {
		return fmt.Errorf("verify signature: %w", err)
	}
	if !ok {
		return errors.New("card signature does not match created_by key")
	}
	return nil
}
