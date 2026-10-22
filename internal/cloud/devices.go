// SPDX-License-Identifier: AGPL-3.0-or-later

package cloud

import (
	"errors"
	"time"
)

var (
	ErrDeviceKeyRequired = errors.New("device key required")
	ErrDeviceUnlinked    = errors.New("device unlinked")
	ErrDeviceKeyIssued   = errors.New("device key already issued")
)

type Device struct {
	SubID    string `json:"sub_id"`
	Label    string `json:"label"`
	LastSeen int64  `json:"last_seen"`
}

type deviceEntry struct {
	Device
	KeyHash string `json:"key_hash,omitempty"`
}

func publicDevices(entries []deviceEntry) []Device {
	out := make([]Device, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Device)
	}
	return out
}

func (v *vaultStore) LinkDevice(id, secret, subID, label string) ([]Device, string, bool, error) {
	unlock := v.locks.lock(id)
	defer unlock()
	m, err := v.authed(id, secret)
	if err != nil {
		return nil, "", false, err
	}
	key, err := newDeviceKey()
	if err != nil {
		return nil, "", false, err
	}
	keyHash := hashSecret(key)
	now := time.Now().UnixMilli()
	found := false
	for i := range m.Devices {
		if m.Devices[i].SubID == subID {
			m.Devices[i].Label = label
			m.Devices[i].LastSeen = now
			m.Devices[i].KeyHash = keyHash
			found = true
		}
	}
	if !found {
		m.Devices = append(m.Devices, deviceEntry{Device: Device{SubID: subID, Label: label, LastSeen: now}, KeyHash: keyHash})
	}
	m.GraceStartedAt = 0
	if err := v.writeMeta(id, m); err != nil {
		return nil, "", false, err
	}
	return publicDevices(m.Devices), key, found, nil
}

func (v *vaultStore) IssueDeviceKey(id, secret, subID string) (string, error) {
	unlock := v.locks.lock(id)
	defer unlock()
	m, err := v.authed(id, secret)
	if err != nil {
		return "", err
	}
	found := false
	for _, d := range m.Devices {
		if d.SubID != subID {
			continue
		}
		if d.KeyHash != "" {
			return "", ErrDeviceKeyIssued
		}
		found = true
	}
	if !found {
		return "", ErrDeviceUnlinked
	}
	key, err := newDeviceKey()
	if err != nil {
		return "", err
	}
	keyHash := hashSecret(key)
	for i := range m.Devices {
		if m.Devices[i].SubID == subID {
			m.Devices[i].KeyHash = keyHash
		}
	}
	if err := v.writeMeta(id, m); err != nil {
		return "", err
	}
	return key, nil
}

func (v *vaultStore) CheckDevice(id, secret, subID, key string) error {
	unlock := v.locks.lock(id)
	defer unlock()
	m, err := v.authed(id, secret)
	if err != nil {
		return err
	}
	if subID == "" || key == "" {
		return ErrDeviceKeyRequired
	}
	for _, d := range m.Devices {
		if d.SubID == subID && d.KeyHash != "" && validHex32(key) && secretMatches(d.KeyHash, key) {
			return nil
		}
	}
	return ErrDeviceUnlinked
}

func (v *vaultStore) UnlinkDevice(id, secret, subID string) ([]Device, error) {
	unlock := v.locks.lock(id)
	defer unlock()
	m, err := v.authed(id, secret)
	if err != nil {
		return nil, err
	}
	kept := m.Devices[:0]
	for _, d := range m.Devices {
		if d.SubID != subID {
			kept = append(kept, d)
		}
	}
	m.Devices = kept
	if len(m.Devices) == 0 && m.GraceStartedAt == 0 {
		m.GraceStartedAt = time.Now().UnixMilli()
	}
	if err := v.writeMeta(id, m); err != nil {
		return nil, err
	}
	return publicDevices(m.Devices), nil
}
