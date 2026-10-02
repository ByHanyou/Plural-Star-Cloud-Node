package network

import (
	"crypto/rand"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
)

func TestNetworkCardSignVerify(t *testing.T) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	card := NetworkCard{
		ID:             "plural-star-global",
		Name:           "Plural Star Global",
		Description:    "The main open network",
		BootstrapPeers: []string{"/ip4/1.2.3.4/tcp/4001/p2p/12D3KooWGFEV2PobB8q33b9MW5sCeKtpfTdWHyPmoV6sAYqZHcCU"},
		NodeCountHint:  0,
		CreatedAt:      1719200000,
	}
	if err := SignNetworkCard(&card, priv); err != nil {
		t.Fatalf("sign: %v", err)
	}
	if card.Signature == "" || card.CreatedBy == "" {
		t.Fatal("signing did not populate signature/created_by")
	}
	if err := VerifyNetworkCard(&card); err != nil {
		t.Fatalf("verify should pass: %v", err)
	}

	tampered := card
	tampered.Description = "evil"
	if err := VerifyNetworkCard(&tampered); err == nil {
		t.Fatal("verify should fail on tampered card")
	}
}

func TestStorePutListGet(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir + "/networks.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	card := NetworkCard{ID: "net-1", Name: "One", CreatedAt: 100}
	if err := store.Put(card); err != nil {
		t.Fatal(err)
	}
	got, found, err := store.Get("net-1")
	if err != nil || !found {
		t.Fatalf("get: found=%v err=%v", found, err)
	}
	if got.Name != "One" {
		t.Fatalf("name mismatch: %q", got.Name)
	}
	list, err := store.List()
	if err != nil || len(list) != 1 {
		t.Fatalf("list: len=%d err=%v", len(list), err)
	}
}
