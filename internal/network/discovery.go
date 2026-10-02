// SPDX-License-Identifier: AGPL-3.0-or-later

package network

import (
	"context"
	"log"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	corenet "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
)

const mdnsServiceTag = "plural-star"

func NewGossipSub(ctx context.Context, h host.Host, kdht *dht.IpfsDHT) (*pubsub.PubSub, *drouting.RoutingDiscovery, error) {
	var (
		opts []pubsub.Option
		rd   *drouting.RoutingDiscovery
	)
	if kdht != nil {
		rd = drouting.NewRoutingDiscovery(kdht)
		opts = append(opts, pubsub.WithDiscovery(rd))
	}
	ps, err := pubsub.NewGossipSub(ctx, h, opts...)
	if err != nil {
		return nil, nil, err
	}
	return ps, rd, nil
}

func AdvertiseAndDiscover(ctx context.Context, h host.Host, rd *drouting.RoutingDiscovery, rendezvous string) {
	if rd == nil {
		return
	}
	dutil.Advertise(ctx, rd, rendezvous)
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			findAndConnect(ctx, h, rd, rendezvous)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func findAndConnect(ctx context.Context, h host.Host, rd *drouting.RoutingDiscovery, rendezvous string) {
	peerCh, err := rd.FindPeers(ctx, rendezvous)
	if err != nil {
		log.Printf("discovery: find peers: %v", err)
		return
	}
	for ai := range peerCh {
		if ai.ID == h.ID() || len(ai.Addrs) == 0 {
			continue
		}
		if h.Network().Connectedness(ai.ID) == corenet.Connected {
			continue
		}
		dialCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		if err := h.Connect(dialCtx, ai); err == nil {
			log.Printf("discovery: connected to %s", ai.ID)
		}
		cancel()
	}
}

type mdnsNotifee struct {
	h   host.Host
	ctx context.Context
}

func (n *mdnsNotifee) HandlePeerFound(pi peer.AddrInfo) {
	if pi.ID == n.h.ID() {
		return
	}
	dialCtx, cancel := context.WithTimeout(n.ctx, 15*time.Second)
	defer cancel()
	if err := n.h.Connect(dialCtx, pi); err == nil {
		log.Printf("mdns: connected to LAN peer %s", pi.ID)
	}
}

func SetupMDNS(ctx context.Context, h host.Host) (mdns.Service, error) {
	svc := mdns.NewMdnsService(h, mdnsServiceTag, &mdnsNotifee{h: h, ctx: ctx})
	if err := svc.Start(); err != nil {
		return nil, err
	}
	return svc, nil
}
