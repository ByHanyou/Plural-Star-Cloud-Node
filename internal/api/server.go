// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
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

type Relay interface {
	Route(*relay.Packet) (relay.Outcome, error)
	Router() *relay.Router
	AppConnected(peer.ID, relay.DeliverFunc)
	AppDisconnected(peer.ID)
}

type Ping interface {
	Count() int
	Nodes() []ping.NodeInfo
}

type Deps struct {
	Hub        *Hub
	Rendezvous *RendezvousStore
	Ping       Ping
	Relay      Relay
	Networks   *network.Store
	Cloud      *cloud.Service
	Gossip     *relay.RendezvousGossip
}

type Server struct {
	cfgMu      sync.RWMutex
	cfg        *config.Config
	inviteMu   sync.Mutex
	configPath string
	h          host.Host
	hub        *Hub
	rv         *RendezvousStore
	ping       Ping
	relay      Relay
	networks   *network.Store
	cloud      *cloud.Service
	gossip     *relay.RendezvousGossip
	startedAt  time.Time

	httpSrv  *http.Server
	upgrader websocket.Upgrader
}

func New(cfg *config.Config, configPath string, h host.Host, d Deps) (*Server, error) {
	if cfg == nil {
		return nil, errors.New("api: config is required")
	}
	if h == nil {
		return nil, errors.New("api: host is required")
	}
	if d.Hub == nil || d.Rendezvous == nil || d.Ping == nil || d.Relay == nil {
		return nil, errors.New("api: hub, rendezvous store, ping and relay are required")
	}
	s := &Server{
		cfg:        cfg,
		configPath: configPath,
		h:          h,
		hub:        d.Hub,
		rv:         d.Rendezvous,
		ping:       d.Ping,
		relay:      d.Relay,
		networks:   d.Networks,
		cloud:      d.Cloud,
		gossip:     d.Gossip,
		startedAt:  time.Now(),
		upgrader: websocket.Upgrader{
			CheckOrigin: originAllowed,
		},
	}
	apiHost := cfg.APIHost
	if apiHost == "" {
		apiHost = "127.0.0.1"
	}
	s.httpSrv = &http.Server{
		Addr:              net.JoinHostPort(apiHost, strconv.Itoa(cfg.APIPort)),
		Handler:           s.routes(),
		ReadHeaderTimeout: cfg.Limits.HTTPReadHeaderTimeout.Std(),
		IdleTimeout:       cfg.Limits.HTTPIdleTimeout.Std(),
	}
	return s, nil
}

func (s *Server) config() *config.Config {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	return s.cfg
}

func (s *Server) setConfig(cfg *config.Config) {
	s.cfgMu.Lock()
	s.cfg = cfg
	s.cfgMu.Unlock()
}

func (s *Server) RendezvousReannounceLoop(ctx context.Context) {
	if s.gossip == nil {
		return
	}
	ticker := time.NewTicker(s.config().Limits.RendezvousReannounceInterval.Std())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, e := range s.rv.localEntries() {
				if err := s.gossip.Announce(e.Namespace, e.Record, e.ExpiresAt); err != nil {
					slog.Warn("rendezvous: re-announce failed", "err", err)
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

func (s *Server) startDebugServer() {
	addr := strings.TrimSpace(s.config().DebugAddr)
	if addr == "" {
		return
	}
	h, _, err := net.SplitHostPort(addr)
	if err != nil {
		slog.Warn("debug_addr is not host:port, pprof not started", "debug_addr", addr, "err", err)
		return
	}
	if ip := net.ParseIP(h); ip == nil || !ip.IsLoopback() {
		slog.Warn("debug_addr is not a loopback address, pprof not started (use an SSH tunnel)", "debug_addr", addr)
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	go func() {
		slog.Info("pprof listening (loopback only)", "url", "http://"+addr+"/debug/pprof/")
		srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: s.config().Limits.HTTPReadHeaderTimeout.Std()}
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Warn("pprof server stopped", "err", err)
		}
	}()
}

func (s *Server) Start() error {
	s.startDebugServer()
	slog.Info("API server listening", "url", "http://"+s.httpSrv.Addr)
	if err := s.httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.hub.closeAll()
	return s.httpSrv.Shutdown(ctx)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg, "code": cloud.CodeForStatus(status)})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, limit int, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, int64(limit))
	err := json.NewDecoder(r.Body).Decode(v)
	if err == nil {
		return true
	}
	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
	} else {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
	}
	return false
}
