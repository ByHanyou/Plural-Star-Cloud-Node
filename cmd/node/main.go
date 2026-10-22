// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/api"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/cloud"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
	psnhost "github.com/ByHanyou/Plural-Star-Cloud-Node/internal/host"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/network"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/ping"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/relay"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/version"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config.yaml")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Printf("%s %s\n", version.AgentName, version.Version)
		return
	}
	if err := run(*configPath); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func logLevel(name string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case config.LogLevelDebug:
		return slog.LevelDebug
	case config.LogLevelWarn:
		return slog.LevelWarn
	case config.LogLevelError:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func setupLogging(level string) {
	lvl := logLevel(level)
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})))
	slog.SetLogLoggerLevel(lvl)
}

func run(configPath string) error {
	cfg, firstRun, err := config.Load(configPath)
	if err != nil {
		return err
	}
	setupLogging(cfg.LogLevel)
	slog.Info("starting", "version", version.Version, "config", configPath, "log_level", cfg.LogLevel)
	if firstRun {
		printFirstRun(configPath, cfg.APIToken)
	}
	lim := cfg.Limits

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

	var cloudSvc *cloud.Service
	pushPub := ""
	if cfg.Cloud.Enabled {
		svc, cErr := cloud.New(ctx, cfg.Cloud, lim, filepath.Dir(configPath))
		if cErr != nil {
			return fmt.Errorf("cloud services: %w", cErr)
		}
		cloudSvc = svc
		pushPub = svc.PushBoxPublicKey()
	}
	agent := version.AgentString(cfg.Roles(), pushPub)

	h, err := psnhost.New(cfg, priv, psk, agent)
	if err != nil {
		return err
	}
	defer h.Close()

	slog.Info("node identity", "peer_id", h.ID())
	slog.Info("network mode", "mode", cfg.NetworkMode, "scope", network.Scope(cfg))
	slog.Info("agent", "agent", agent)
	for _, a := range h.Addrs() {
		slog.Info("listening", "addr", fmt.Sprintf("%s/p2p/%s", a, h.ID()))
	}

	var kdht *dht.IpfsDHT
	if cfg.NetworkMode != config.ModePrivate {
		d, dErr := psnhost.NewDHT(ctx, h, network.DHTPrefix(cfg))
		if dErr != nil {
			return dErr
		}
		defer d.Close()
		kdht = d
		slog.Info("DHT started", "prefix", network.DHTPrefix(cfg))
	}

	ps, rd, gErr := network.NewGossipSub(ctx, h, kdht)
	if gErr != nil {
		return fmt.Errorf("gossipsub: %w", gErr)
	}
	slog.Info("gossipsub ready")

	if rd != nil {
		network.AdvertiseAndDiscover(ctx, h, rd, network.DHTPrefix(cfg), lim.DiscoveryInterval.Std(), lim.DiscoveryDialTimeout.Std())
		slog.Info("DHT peer discovery advertising", "prefix", network.DHTPrefix(cfg))
	}

	mdnsSvc, mErr := network.SetupMDNS(ctx, h, lim.DialTimeout.Std())
	if mErr != nil {
		slog.Warn("mDNS unavailable", "err", mErr)
	} else {
		defer mdnsSvc.Close()
		slog.Info("mDNS LAN discovery started")
	}

	hub := api.NewHub()
	rv := api.NewRendezvousStore(ctx, configPath, lim)

	pingMgr := ping.NewManager(ctx, h, lim.PingTimeout.Std(), hub.OnNodeEvent)

	mgr, rErr := relay.NewManager(ctx, h, ps, network.GossipPrefix(cfg), lim, hub.OnAppPeerEvent)
	if rErr != nil {
		return fmt.Errorf("relay manager: %w", rErr)
	}
	slog.Info("relay protocol registered", "protocol", relay.RelayProtocol)

	rvTopic := network.GossipPrefix(cfg) + "rendezvous"
	rvGossip, rvErr := relay.NewRendezvousGossip(ctx, ps, h.ID(), rvTopic, lim.RendezvousMaxNamespaceLen, lim.RendezvousMaxRecordBytes, lim.RendezvousMaxTTL.Std(), rv.PutRemote)
	if rvErr != nil {
		return fmt.Errorf("rendezvous gossip: %w", rvErr)
	}
	slog.Info("rendezvous gossip joined", "topic", rvTopic)

	if cloudSvc != nil {
		mgr.SetOffline(cloudSvc.Inbox().Store)
		mgr.SetOfflineReplay(cloudSvc.Inbox().Replay)
		slog.Info("cloud services attached", "roles", cfg.Roles())
	}

	var networks *network.Store
	if cfg.NetworkMode != config.ModePrivate {
		store, sErr := network.OpenStore(network.NetworkDBDefault)
		if sErr != nil {
			return fmt.Errorf("network store: %w", sErr)
		}
		defer store.Close()
		networks = store

		if _, ndErr := network.NewNetworkDiscovery(ctx, ps, h.ID(), store); ndErr != nil {
			return fmt.Errorf("network discovery: %w", ndErr)
		}
		slog.Info("network discovery joined", "topic", network.DiscoveryTopic)

		go func() {
			if count, fErr := network.FetchDirectory(ctx, cfg.DirectoryURL, store, lim.DirectoryFetchTimeout.Std()); fErr != nil {
				slog.Warn("directory fetch failed", "url", cfg.DirectoryURL, "err", fErr)
			} else if count > 0 {
				slog.Info("directory cached", "cards", count)
			}
		}()
	}

	srv, aErr := api.New(cfg, configPath, h, api.Deps{
		Hub:        hub,
		Rendezvous: rv,
		Ping:       pingMgr,
		Relay:      mgr,
		Networks:   networks,
		Cloud:      cloudSvc,
		Gossip:     rvGossip,
	})
	if aErr != nil {
		return aErr
	}
	go srv.RendezvousReannounceLoop(ctx)

	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.Start() }()
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), lim.ShutdownTimeout.Std())
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			slog.Warn("API server shutdown", "err", err)
		}
	}()

	peerMem := psnhost.LoadPeerMemory(filepath.Join(filepath.Dir(configPath), "known_peers.json"), lim.PeerMemoryMaxPeers, lim.PeerMemoryMaxAddrs, lim.PeerMemoryTTL.Std())

	bootstrapPeers := network.BootstrapPeers(cfg)
	bootstrapInfos, bpErr := psnhost.ParsePeerAddrs(bootstrapPeers)
	if bpErr != nil {
		return bpErr
	}
	remembered := peerMem.DialTargets(h.ID())
	targets := psnhost.MergeTargets(bootstrapInfos, remembered)
	if len(targets) > 0 {
		connected, bErr := psnhost.ConnectPeers(ctx, h, targets, lim.DialConcurrency, lim.DialTimeout.Std())
		slog.Info("initial bootstrap", "connected", connected, "targets", len(targets), "remembered", len(remembered))
		if bErr != nil {
			slog.Warn("initial bootstrap partial", "err", bErr)
		}
		peerMem.Snapshot(h)
		peerMem.Save()
	} else if cfg.NetworkMode == config.ModePublic {
		slog.Warn("no bootstrap peers and no remembered peers")
	}

	go persistentReconnectLoop(ctx, h, bootstrapInfos, peerMem, lim)

	go monitorConnections(ctx, h, lim.ConnectionMonitorInterval.Std())

	slog.Info("node running; press Ctrl-C to stop")
	select {
	case <-ctx.Done():
	case err := <-srvErr:
		return fmt.Errorf("API server: %w", err)
	}
	slog.Info("shutting down")
	peerMem.Snapshot(h)
	peerMem.Save()
	return nil
}

func persistentReconnectLoop(ctx context.Context, h host.Host, bootstrapInfos []peer.AddrInfo, peerMem *psnhost.PeerMemory, lim config.Limits) {
	ticker := time.NewTicker(lim.ReconnectInterval.Std())
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
			connected, err := psnhost.ConnectPeers(ctx, h, targets, lim.DialConcurrency, lim.DialTimeout.Std())
			if connected > 0 {
				slog.Debug("reconnect", "connected", connected)
			} else if err != nil {
				slog.Warn("reconnect failed", "err", err)
			}
		}
	}
}

func monitorConnections(ctx context.Context, h host.Host, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			conns := h.Network().Conns()
			slog.Debug("active libp2p connections", "count", len(conns))
		}
	}
}

func printFirstRun(configPath, token string) {
	fmt.Println("=====================================================")
	fmt.Println(" Plural Star Cloud Node - first run")
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
