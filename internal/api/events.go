// SPDX-License-Identifier: AGPL-3.0-or-later

package api

type peerOnlineEvent struct {
	Type    string `json:"type"`
	PeerID  string `json:"peer_id"`
	ViaNode string `json:"via_node"`
}

type peerOfflineEvent struct {
	Type   string `json:"type"`
	PeerID string `json:"peer_id"`
}

type nodeConnectedEvent struct {
	Type       string `json:"type"`
	NodePeerID string `json:"node_peer_id"`
	RTTms      int64  `json:"rtt_ms"`
}

type nodeDisconnectedEvent struct {
	Type       string `json:"type"`
	NodePeerID string `json:"node_peer_id"`
}

type packetReceivedEvent struct {
	Type         string `json:"type"`
	SenderPeerID string `json:"sender_peer_id"`
	Payload      string `json:"payload"`
	Timestamp    int64  `json:"timestamp"`
}

type errorEvent struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}
