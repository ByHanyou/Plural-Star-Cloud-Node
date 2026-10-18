// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/api"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/cloud"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
	psnhost "github.com/ByHanyou/Plural-Star-Cloud-Node/internal/host"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/network"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/ping"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/relay"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
)

type node struct {
	cfg      *config.Config
	h        host.Host
	dht      *dht.IpfsDHT
	ps       *pubsub.PubSub
	rd       *drouting.RoutingDiscovery
	relay    *relay.Manager
	ping     *ping.Manager
	api      *api.Server
	cloud    *cloud.Service
	networks *network.Store
}

func main() {
	configPath := flag.String("config", "config.yaml", "path to config.yaml")
	flag.Parse()
	if err := run(*configPath); err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

func run(configPath string) error {
	cfg, firstRun, err := config.Load(configPath)
	if err != nil {
		return err
	}
	if firstRun {
		printFirstRun(configPath, cfg.APIToken)
	}

	var priv crypto.PrivKey
	if cfg.RequireExistingIdentity {
		priv, err = psnhost.LoadIdentity(cfg.KeypairPath)
	} else {
		priv, err = psnhost.LoadOrCreateIdentity(cfg.KeypairPath)
	}
	if err != nil {
		return err
	}

	var psk []byte
	if cfg.NetworkMode == config.ModePrivate {
		psk, err = network.LoadPSK(cfg.PSKPath)
		if err != nil {
			return err
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	n := &node{cfg: cfg}
	pushPub := ""
	if cfg.Cloud.Enabled {
		svc, cErr := cloud.New(ctx, cfg.Cloud, filepath.Dir(configPath))
		if cErr != nil {
			return fmt.Errorf("cloud services: %w", cErr)
		}
		n.cloud = svc
		pushPub = svc.PushBoxPublicKey()
	}
	agent := cloud.AgentString(cfg.Roles(), pushPub)

	h, err := psnhost.New(cfg, priv, psk, agent)
	if err != nil {
		return err
	}
	defer h.Close()
	n.h = h

	log.Printf("node peer ID: %s", h.ID())
	log.Printf("network mode: %s (scope %q)", cfg.NetworkMode, network.Scope(cfg))
	log.Printf("agent: %s", agent)
	for _, a := range h.Addrs() {
		log.Printf("listening on: %s/p2p/%s", a, h.ID())
	}

	if cfg.NetworkMode != config.ModePrivate {
		kdht, dErr := psnhost.NewDHT(ctx, h, network.DHTPrefix(cfg))
		if dErr != nil {
			return dErr
		}
		defer kdht.Close()
		n.dht = kdht
		log.Printf("DHT started (prefix %s)", network.DHTPrefix(cfg))
	}

	ps, rd, gErr := network.NewGossipSub(ctx, h, n.dht)
	if gErr != nil {
		return fmt.Errorf("gossipsub: %w", gErr)
	}
	n.ps, n.rd = ps, rd
	log.Printf("gossipsub ready")

	if n.rd != nil {
		network.AdvertiseAndDiscover(ctx, h, n.rd, network.DHTPrefix(cfg))
		log.Printf("DHT peer discovery advertising %q", network.DHTPrefix(cfg))
	}

	mdnsSvc, mErr := network.SetupMDNS(ctx, h)
	if mErr != nil {
		log.Printf("warning: mDNS unavailable: %v", mErr)
	} else {
		defer mdnsSvc.Close()
		log.Printf("mDNS LAN discovery started")
	}

	srv := api.NewServer(cfg, configPath, h, network.Scope(cfg))
	n.api = srv

	pingMgr := ping.NewManager(ctx, h, srv.OnNodeEvent)
	n.ping = pingMgr
	srv.SetPing(pingMgr)

	mgr, rErr := relay.NewManager(ctx, h, n.ps, network.GossipPrefix(cfg), srv.OnAppPeerEvent)
	if rErr != nil {
		return fmt.Errorf("relay manager: %w", rErr)
	}
	n.relay = mgr
	srv.SetRelay(mgr)
	log.Printf("relay protocol %s registered", relay.RelayProtocol)

	rvGossip, rvErr := relay.NewRendezvousGossip(ctx, n.ps, h.ID(), network.GossipPrefix(cfg)+"rendezvous", srv.OnRemoteRendezvous)
	if rvErr != nil {
		return fmt.Errorf("rendezvous gossip: %w", rvErr)
	}
	srv.SetRendezvousGossip(rvGossip)
	go srv.RendezvousReannounceLoop(ctx)
	log.Printf("rendezvous gossip joined %q", network.GossipPrefix(cfg)+"rendezvous")

	if n.cloud != nil {
		srv.SetCloud(n.cloud)
		mgr.SetOffline(n.cloud.Inbox().Store)
		mgr.SetOfflineReplay(n.cloud.Inbox().Replay)
		log.Printf("cloud services attached, roles %v", cfg.Roles())
	}

	if cfg.NetworkMode != config.ModePrivate {
		store, sErr := network.OpenStore(network.NetworkDBDefault)
		if sErr != nil {
			return fmt.Errorf("network store: %w", sErr)
		}
		defer store.Close()
		n.networks = store
		srv.SetNetworks(store)

		if _, ndErr := network.NewNetworkDiscovery(ctx, n.ps, h.ID(), store); ndErr != nil {
			return fmt.Errorf("network discovery: %w", ndErr)
		}
		log.Printf("network discovery joined %q", network.DiscoveryTopic)

		directoryURL := cfg.DirectoryURL
		if directoryURL == "" {
			directoryURL = network.DefaultDirectoryURL
		}
		go func() {
			if count, fErr := network.FetchDirectory(ctx, directoryURL, store); fErr != nil {
				log.Printf("directory fetch skipped: %v", fErr)
			} else if count > 0 {
				log.Printf("directory: cached %d network card(s)", count)
			}
		}()
	}

	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("API server error: %v", err)
		}
	}()
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()

	peerMem := psnhost.LoadPeerMemory(filepath.Join(filepath.Dir(configPath), "known_peers.json"))

	bootstrapPeers := network.BootstrapPeers(cfg)
	bootstrapInfos, bpErr := psnhost.ParsePeerAddrs(bootstrapPeers)
	if bpErr != nil {
		return bpErr
	}
	remembered := peerMem.DialTargets(h.ID())
	targets := psnhost.MergeTargets(bootstrapInfos, remembered)
	if len(targets) > 0 {
		connected, bErr := psnhost.ConnectPeers(ctx, h, targets)
		log.Printf("initial bootstrap: connected to %d/%d peers (%d remembered)", connected, len(targets), len(remembered))
		if bErr != nil {
			log.Printf("initial bootstrap partial: %v", bErr)
		}
		peerMem.Snapshot(h)
		peerMem.Save()
	} else if cfg.NetworkMode == config.ModePublic {
		log.Printf("warning: no bootstrap peers and no remembered peers")
	}

	go persistentReconnectLoop(ctx, h, bootstrapInfos, peerMem)

	go monitorConnections(ctx, h)

	log.Printf("node running; press Ctrl-C to stop")
	<-ctx.Done()
	log.Printf("shutting down")
	peerMem.Snapshot(h)
	peerMem.Save()
	return nil
}

func persistentReconnectLoop(ctx context.Context, h host.Host, bootstrapInfos []peer.AddrInfo, peerMem *psnhost.PeerMemory) {
	ticker := time.NewTicker(90 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			peerMem.Snapshot(h)
			peerMem.Save()
			targets := psnhost.MergeTargets(bootstrapInfos, peerMem.DialTargets(h.ID()))
			if len(targets) == 0 {
				continue
			}
			connected, err := psnhost.ConnectPeers(ctx, h, targets)
			if connected > 0 {
				log.Printf("reconnect success: %d peers", connected)
			} else if err != nil {
				log.Printf("reconnect failed: %v", err)
			}
		}
	}
}

func monitorConnections(ctx context.Context, h host.Host) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			conns := h.Network().Conns()
			log.Printf("active libp2p connections: %d", len(conns))
		}
	}
}

func printFirstRun(configPath, token string) {
	fmt.Println("=====================================================")
	fmt.Println(" Plural Star Node - first run")
	fmt.Printf(" Wrote default config to: %s\n", configPath)
	if token == "" {
		fmt.Println(" API auth is open, which is correct for a public node: apps on the")
		fmt.Println(" default network have no token. api_token is for private and")
		fmt.Println(" custom_public networks only, and is ignored here.")
	} else {
		fmt.Printf(" API token (configure this in your Plural Star app):\n   %s\n", token)
	}
	fmt.Println("=====================================================")
}
