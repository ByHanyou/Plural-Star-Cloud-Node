// SPDX-License-Identifier: AGPL-3.0-or-later

package network

import (
	"context"
	"log/slog"
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

func AdvertiseAndDiscover(ctx context.Context, h host.Host, rd *drouting.RoutingDiscovery, rendezvous string, interval, dialTimeout time.Duration) {
	if rd == nil {
		return
	}
	dutil.Advertise(ctx, rd, rendezvous)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			findAndConnect(ctx, h, rd, rendezvous, dialTimeout)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func findAndConnect(ctx context.Context, h host.Host, rd *drouting.RoutingDiscovery, rendezvous string, dialTimeout time.Duration) {
	peerCh, err := rd.FindPeers(ctx, rendezvous)
	if err != nil {
		slog.Warn("discovery: find peers failed", "err", err)
		return
	}
	for ai := range peerCh {
		if ai.ID == h.ID() || len(ai.Addrs) == 0 {
			continue
		}
		if h.Network().Connectedness(ai.ID) == corenet.Connected {
			continue
		}
		dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
		if err := h.Connect(dialCtx, ai); err != nil {
			slog.Debug("discovery: dial failed", "peer", ai.ID, "err", err)
		} else {
			slog.Debug("discovery: connected", "peer", ai.ID)
		}
		cancel()
	}
}

type mdnsNotifee struct {
	dialTimeout time.Duration
	h           host.Host
	ctx         context.Context
}

func (n *mdnsNotifee) HandlePeerFound(pi peer.AddrInfo) {
	if pi.ID == n.h.ID() {
		return
	}
	dialCtx, cancel := context.WithTimeout(n.ctx, n.dialTimeout)
	defer cancel()
	if err := n.h.Connect(dialCtx, pi); err != nil {
		slog.Debug("mdns: dial failed", "peer", pi.ID, "err", err)
		return
	}
	slog.Debug("mdns: connected to LAN peer", "peer", pi.ID)
}

func SetupMDNS(ctx context.Context, h host.Host, dialTimeout time.Duration) (mdns.Service, error) {
	svc := mdns.NewMdnsService(h, mdnsServiceTag, &mdnsNotifee{h: h, ctx: ctx, dialTimeout: dialTimeout})
	if err := svc.Start(); err != nil {
		return nil, err
	}
	return svc, nil
}
