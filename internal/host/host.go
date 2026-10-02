// SPDX-License-Identifier: AGPL-3.0-or-later

package host

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	libp2pnetwork "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	tcp "github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"
)

// LoadIdentity reads an existing keypair and refuses to invent one. Use it
// wherever the peer ID is already known to the outside world.
func LoadIdentity(path string) (crypto.PrivKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("keypair %q does not exist and require_existing_identity is set: refusing to generate a new peer ID (copy node.key into place, or unset the flag for a fresh node)", path)
		}
		return nil, fmt.Errorf("read keypair %q: %w", path, err)
	}
	priv, uErr := crypto.UnmarshalPrivateKey(data)
	if uErr != nil {
		return nil, fmt.Errorf("parse keypair %q: %w", path, uErr)
	}
	return priv, nil
}

func LoadOrCreateIdentity(path string) (crypto.PrivKey, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		priv, uErr := crypto.UnmarshalPrivateKey(data)
		if uErr != nil {
			return nil, fmt.Errorf("parse keypair %q: %w", path, uErr)
		}
		return priv, nil
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read keypair %q: %w", path, err)
	}

	priv, _, gErr := crypto.GenerateEd25519Key(rand.Reader)
	if gErr != nil {
		return nil, fmt.Errorf("generate keypair: %w", gErr)
	}
	out, mErr := crypto.MarshalPrivateKey(priv)
	if mErr != nil {
		return nil, fmt.Errorf("marshal keypair: %w", mErr)
	}
	if wErr := os.WriteFile(path, out, 0o600); wErr != nil {
		return nil, fmt.Errorf("write keypair %q: %w", path, wErr)
	}
	return priv, nil
}

// New builds the libp2p host. agent is advertised through the identify
// protocol as this node's AgentVersion, which is how mesh peers learn each
// other's roles and push box key without a second exchange; empty keeps
// libp2p's default.
func New(cfg *config.Config, priv crypto.PrivKey, psk []byte, agent string) (host.Host, error) {
	listen, err := parseMultiaddrs(cfg.ListenAddrs)
	if err != nil {
		return nil, fmt.Errorf("listen_addrs: %w", err)
	}

	cm, err := connmgr.NewConnManager(
		cfg.MaxPeers/2,
		cfg.MaxPeers,
		connmgr.WithGracePeriod(time.Minute),
	)
	if err != nil {
		return nil, fmt.Errorf("connection manager: %w", err)
	}

	if psk != nil {
		listen = dropQUIC(listen)
	}

	opts := []libp2p.Option{
		libp2p.Identity(priv),
		libp2p.ListenAddrs(listen...),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer(yamux.ID, yamux.DefaultTransport),
		libp2p.ConnectionManager(cm),
		libp2p.EnableNATService(),
		libp2p.EnableHolePunching(),
		libp2p.EnableRelay(),
	}
	if psk == nil {
		opts = append(opts, libp2p.Transport(libp2pquic.NewTransport))
	}
	if agent != "" {
		opts = append(opts, libp2p.UserAgent(agent))
	}

	if cfg.RelayEnabled {
		opts = append(opts, libp2p.EnableRelayService())
	}

	if len(cfg.AnnounceAddrs) > 0 {
		announce, aErr := parseMultiaddrs(cfg.AnnounceAddrs)
		if aErr != nil {
			return nil, fmt.Errorf("announce_addrs: %w", aErr)
		}
		opts = append(opts, libp2p.AddrsFactory(func([]ma.Multiaddr) []ma.Multiaddr {
			return announce
		}))
	}

	if psk != nil {
		opts = append(opts, libp2p.PrivateNetwork(psk))
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("build libp2p host: %w", err)
	}
	return h, nil
}

func NewDHT(ctx context.Context, h host.Host, prefix string) (*dht.IpfsDHT, error) {
	kdht, err := dht.New(ctx, h,
		dht.Mode(dht.ModeServer),
		dht.ProtocolPrefix(protocol.ID(prefix)),
	)
	if err != nil {
		return nil, fmt.Errorf("create dht: %w", err)
	}
	if err := kdht.Bootstrap(ctx); err != nil {
		return nil, fmt.Errorf("bootstrap dht: %w", err)
	}
	return kdht, nil
}

// ConnectPeers dials every target we are not already connected to and reports
// how many NEW connections were made. Skipping live connections keeps the
// reconnect loop's count honest and its log quiet while the mesh is healthy.
func ConnectPeers(ctx context.Context, h host.Host, infos []peer.AddrInfo) (int, error) {
	var (
		mu        sync.Mutex
		wg        sync.WaitGroup
		connected int
		firstErr  error
	)
	sem := make(chan struct{}, 8)
	for _, ai := range infos {
		if ai.ID == h.ID() {
			continue
		}
		if h.Network().Connectedness(ai.ID) == libp2pnetwork.Connected {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(ai peer.AddrInfo) {
			defer wg.Done()
			defer func() { <-sem }()
			dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			cErr := h.Connect(dialCtx, ai)
			mu.Lock()
			defer mu.Unlock()
			if cErr != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("connect %s: %w", ai.ID, cErr)
				}
				return
			}
			connected++
		}(ai)
	}
	wg.Wait()
	return connected, firstErr
}

func ParsePeerAddrs(addrs []string) ([]peer.AddrInfo, error) {
	out := make([]peer.AddrInfo, 0, len(addrs))
	for _, s := range addrs {
		maddr, err := ma.NewMultiaddr(s)
		if err != nil {
			return nil, fmt.Errorf("parse bootstrap addr %q: %w", s, err)
		}
		ai, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			return nil, fmt.Errorf("bootstrap addr %q missing /p2p component: %w", s, err)
		}
		out = append(out, *ai)
	}
	return out, nil
}

func dropQUIC(addrs []ma.Multiaddr) []ma.Multiaddr {
	out := make([]ma.Multiaddr, 0, len(addrs))
	for _, a := range addrs {
		if strings.Contains(a.String(), "quic") {
			continue
		}
		out = append(out, a)
	}
	return out
}

func parseMultiaddrs(addrs []string) ([]ma.Multiaddr, error) {
	out := make([]ma.Multiaddr, 0, len(addrs))
	for _, s := range addrs {
		maddr, err := ma.NewMultiaddr(s)
		if err != nil {
			return nil, fmt.Errorf("parse multiaddr %q: %w", s, err)
		}
		out = append(out, maddr)
	}
	return out, nil
}
