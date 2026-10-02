package network

import (
	"crypto/rand"
	"testing"

	ma "github.com/multiformats/go-multiaddr"
)

func TestInviteRoundTrip(t *testing.T) {
	a1, _ := ma.NewMultiaddr("/ip4/203.0.113.5/tcp/4001/p2p/12D3KooWGFEV2PobB8q33b9MW5sCeKtpfTdWHyPmoV6sAYqZHcCU")
	a2, _ := ma.NewMultiaddr("/ip4/203.0.113.5/udp/4001/quic-v1/p2p/12D3KooWGFEV2PobB8q33b9MW5sCeKtpfTdWHyPmoV6sAYqZHcCU")
	psk := make([]byte, 32)
	if _, err := rand.Read(psk); err != nil {
		t.Fatal(err)
	}

	in := Invite{Multiaddrs: []ma.Multiaddr{a1, a2}, PSK: psk, Label: "my-private-net"}
	enc, err := EncodeInvite(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	dec, err := DecodeInvite(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(dec.Multiaddrs) != 2 {
		t.Fatalf("want 2 addrs, got %d", len(dec.Multiaddrs))
	}
	if dec.Multiaddrs[0].String() != a1.String() || dec.Multiaddrs[1].String() != a2.String() {
		t.Fatalf("addr mismatch: %v", dec.Multiaddrs)
	}
	if string(dec.PSK) != string(psk) {
		t.Fatal("psk mismatch")
	}
	if dec.Label != "my-private-net" {
		t.Fatalf("label mismatch: %q", dec.Label)
	}
}

func TestInviteRejectsGarbage(t *testing.T) {
	if _, err := DecodeInvite("not-an-invite"); err == nil {
		t.Fatal("expected error for non-invite string")
	}
	if _, err := DecodeInvite("psnode://v1/!!!notbase58!!!"); err == nil {
		t.Fatal("expected error for bad base58")
	}
}
