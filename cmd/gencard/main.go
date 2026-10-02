// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/host"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/network"
)

func main() {
	key := flag.String("key", "./node.key", "Ed25519 key used to sign (created if absent)")
	id := flag.String("id", "", "network id, e.g. plural-star-global")
	name := flag.String("name", "", "human-readable network name")
	desc := flag.String("desc", "", "description")
	boot := flag.String("bootstrap", "", "comma-separated bootstrap multiaddrs")
	ncount := flag.Int("node-count", 0, "node_count_hint")
	flag.Parse()

	if *id == "" || *name == "" {
		fmt.Fprintln(os.Stderr, "error: -id and -name are required")
		os.Exit(2)
	}

	priv, err := host.LoadOrCreateIdentity(*key)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	var peers []string
	for _, p := range strings.Split(*boot, ",") {
		if s := strings.TrimSpace(p); s != "" {
			peers = append(peers, s)
		}
	}

	card := network.NetworkCard{
		ID:             *id,
		Name:           *name,
		Description:    *desc,
		BootstrapPeers: peers,
		NodeCountHint:  *ncount,
		CreatedAt:      time.Now().Unix(),
	}
	if err := network.SignNetworkCard(&card, priv); err != nil {
		fmt.Fprintln(os.Stderr, "error: sign:", err)
		os.Exit(1)
	}
	if err := network.VerifyNetworkCard(&card); err != nil {
		fmt.Fprintln(os.Stderr, "error: self-verify failed:", err)
		os.Exit(1)
	}

	out, err := json.MarshalIndent(card, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	fmt.Println(string(out))
}
