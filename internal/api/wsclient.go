// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"log/slog"
	"sync"
	"time"

	"github.com/ByHanyou/Plural-Star-Cloud-Node/internal/config"

	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p/core/peer"
)

const dropLogInterval = time.Minute

type wsClient struct {
	lim         config.Limits
	conn        *websocket.Conn
	appPeer     peer.ID
	send        chan []byte
	mu          sync.Mutex
	closed      bool
	dropped     int64
	lastDropLog time.Time
}

func newWSClient(conn *websocket.Conn, appPeer peer.ID, lim config.Limits) *wsClient {
	return &wsClient{
		conn:    conn,
		appPeer: appPeer,
		lim:     lim,
		send:    make(chan []byte, lim.WSSendBuffer),
	}
}

func (c *wsClient) trySend(b []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	select {
	case c.send <- b:
	default:
		c.dropped++
		if c.lastDropLog.IsZero() || time.Since(c.lastDropLog) >= dropLogInterval {
			slog.Warn("api: websocket send buffer full, event dropped", "peer", c.appPeer, "dropped", c.dropped)
			c.lastDropLog = time.Now()
		}
	}
}

func (c *wsClient) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	close(c.send)
}

func (c *wsClient) writePump() {
	ticker := time.NewTicker(c.lim.WSPingPeriod.Std())
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()
	for {
		select {
		case msg, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.lim.WSWriteWait.Std()))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.lim.WSWriteWait.Std()))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *wsClient) readPump(onClose func()) {
	defer onClose()
	c.conn.SetReadLimit(int64(c.lim.WSMaxMessageBytes))
	_ = c.conn.SetReadDeadline(time.Now().Add(c.lim.WSPongWait.Std()))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(c.lim.WSPongWait.Std()))
	})
	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			return
		}
	}
}
