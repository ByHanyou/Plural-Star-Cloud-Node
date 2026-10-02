// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
	"sync"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/cloud"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/network"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/ping"
	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/relay"

	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

type Server struct {
	cfg        *config.Config
	configPath string
	h          host.Host
	ping       *ping.Manager
	relay      *relay.Manager
	networks   *network.Store
	cloud      *cloud.Service
	scope      string
	startedAt  time.Time

	httpSrv  *http.Server
	upgrader websocket.Upgrader

	mu      sync.RWMutex
	clients map[*wsClient]struct{}
	appPeer peer.ID

	rv *rendezvousStore

	rvGossipMu sync.RWMutex
	rvGossip   *relay.RendezvousGossip
}

func NewServer(cfg *config.Config, configPath string, h host.Host, scope string) *Server {
	return &Server{
		cfg:        cfg,
		configPath: configPath,
		h:          h,
		scope:      scope,
		startedAt:  time.Now(),
		upgrader: websocket.Upgrader{
			CheckOrigin: originAllowed,
		},
		clients: make(map[*wsClient]struct{}),
		rv:      newRendezvousStore(rendezvousPathFor(configPath)),
	}
}

func (s *Server) SetPing(m *ping.Manager) { s.ping = m }

func (s *Server) SetRelay(m *relay.Manager) { s.relay = m }

func (s *Server) SetNetworks(store *network.Store) { s.networks = store }

// SetCloud attaches the cloud services. Must be called before Start, because
// the /cloud/* routes are registered when the mux is built.
func (s *Server) SetCloud(svc *cloud.Service) { s.cloud = svc }

func (s *Server) SetRendezvousGossip(g *relay.RendezvousGossip) {
	s.rvGossipMu.Lock()
	s.rvGossip = g
	s.rvGossipMu.Unlock()
}

func (s *Server) rendezvousGossip() *relay.RendezvousGossip {
	s.rvGossipMu.RLock()
	defer s.rvGossipMu.RUnlock()
	return s.rvGossip
}

func (s *Server) OnRemoteRendezvous(namespace, record string, ttl time.Duration) {
	s.rv.putRemote(namespace, record, ttl)
}

func (s *Server) RendezvousReannounceLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g := s.rendezvousGossip()
			if g == nil {
				continue
			}
			for _, e := range s.rv.localEntries() {
				if err := g.Announce(e.Namespace, e.Record, e.ExpiresAt); err != nil {
					log.Printf("rendezvous: re-announce failed: %v", err)
				}
			}
		}
	}
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.authed(s.handleHealth))
	mux.HandleFunc("/nodes", s.authed(s.handleNodes))
	mux.HandleFunc("/peers", s.authed(s.handlePeers))
	mux.HandleFunc("/networks", s.authed(s.handleNetworks))
	mux.HandleFunc("/send", s.authed(s.handleSend))
	mux.HandleFunc("/invite/generate", s.authed(s.handleInviteGenerate))
	mux.HandleFunc("/invite/accept", s.authed(s.handleInviteAccept))
	mux.HandleFunc("/config", s.authed(s.handleConfig))
	mux.HandleFunc("/rendezvous/register", s.authed(s.handleRendezvousRegister))
	mux.HandleFunc("/rendezvous/lookup", s.authed(s.handleRendezvousLookup))
	mux.HandleFunc("/ws", s.authed(s.handleWS))
	if s.cloud != nil {
		s.cloud.Register(mux, s.authed)
	}
	return mux
}

// startDebugServer exposes pprof on its own loopback-only listener when
// debug_addr is set. It is never attached to the main mux: that one binds
// 0.0.0.0 in production, and pprof there would serve heap and goroutine dumps
// to the internet. Non-loopback addresses are refused rather than trusted.
func (s *Server) startDebugServer() {
	addr := strings.TrimSpace(s.cfg.DebugAddr)
	if addr == "" {
		return
	}
	h, _, err := net.SplitHostPort(addr)
	if err != nil {
		log.Printf("debug_addr %q is not host:port, pprof not started: %v", addr, err)
		return
	}
	if ip := net.ParseIP(h); ip == nil || !ip.IsLoopback() {
		log.Printf("debug_addr %q is not a loopback address, pprof NOT started (use an SSH tunnel)", addr)
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	go func() {
		log.Printf("pprof listening on http://%s/debug/pprof/ (loopback only)", addr)
		srv := &http.Server{Addr: addr, Handler: mux}
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("pprof server stopped: %v", err)
		}
	}()
}

func (s *Server) Start() error {
	s.startDebugServer()
	host := s.cfg.APIHost
	if host == "" {
		host = "127.0.0.1"
	}
	s.httpSrv = &http.Server{
		Addr:              fmt.Sprintf("%s:%d", host, s.cfg.APIPort),
		Handler:           s.routes(),
		ReadHeaderTimeout: 15 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	log.Printf("API server listening on http://%s:%d", host, s.cfg.APIPort)
	if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	for c := range s.clients {
		c.close()
	}
	s.clients = make(map[*wsClient]struct{})
	s.mu.Unlock()
	if s.httpSrv == nil {
		return nil
	}
	return s.httpSrv.Shutdown(ctx)
}

func (s *Server) OnAppPeerEvent(peerID, viaNode peer.ID, online bool) {
	if online {
		s.broadcast(peerOnlineEvent{Type: "peer_online", PeerID: peerID.String(), ViaNode: viaNode.String()})
	} else {
		s.broadcast(peerOfflineEvent{Type: "peer_offline", PeerID: peerID.String()})
	}
}

func (s *Server) OnNodeEvent(peerID peer.ID, rttMs int64, connected bool) {
	if connected {
		s.broadcast(nodeConnectedEvent{Type: "node_connected", NodePeerID: peerID.String(), RTTms: rttMs})
	} else {
		s.broadcast(nodeDisconnectedEvent{Type: "node_disconnected", NodePeerID: peerID.String()})
	}
}

func (s *Server) broadcast(ev any) {
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	s.mu.RLock()
	for c := range s.clients {
		c.trySend(b)
	}
	s.mu.RUnlock()
}

func (s *Server) sendToApp(appPeer peer.ID, ev any) {
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	s.mu.RLock()
	for c := range s.clients {
		if c.appPeer == appPeer {
			c.trySend(b)
		}
	}
	s.mu.RUnlock()
}

func (s *Server) addClient(c *wsClient) {
	s.mu.Lock()
	s.clients[c] = struct{}{}
	s.appPeer = c.appPeer
	s.mu.Unlock()
}

func (s *Server) removeClient(c *wsClient) {
	s.mu.Lock()
	delete(s.clients, c)
	if s.appPeer == c.appPeer {
		s.appPeer = ""
		for other := range s.clients {
			if other.appPeer == c.appPeer {
				s.appPeer = c.appPeer
				break
			}
		}
	}
	s.mu.Unlock()
}

func (s *Server) hasClientFor(appPeer peer.ID) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for c := range s.clients {
		if c.appPeer == appPeer {
			return true
		}
	}
	return false
}

func (s *Server) currentAppPeer() peer.ID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.appPeer
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
