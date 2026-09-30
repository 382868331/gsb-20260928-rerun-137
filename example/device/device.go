package device

import (
	"fmt"

	"replayable-app"
)

const (
	EventRegistered      = "device.registered"
	EventStatusReported  = "device.status_reported"
	EventFirmwareUpdated = "device.firmware_updated"
)

type Registered struct {
	DeviceID string `json:"device_id"`
	Model    string `json:"model"`
}

type StatusReported struct {
	DeviceID  string `json:"device_id"`
	Online    bool   `json:"online"`
	SignalDBM int    `json:"signal_dbm"`
}

type FirmwareUpdated struct {
	DeviceID string `json:"device_id"`
	Firmware string `json:"firmware"`
}

type Device struct {
	ID       string `json:"id"`
	Model    string `json:"model"`
	Online   bool   `json:"online"`
	Signal   int    `json:"signal_dbm"`
	Firmware string `json:"firmware"`
}

type State struct {
	Devices map[string]Device `json:"devices"`
}

func NewState() State {
	return State{Devices: make(map[string]Device)}
}

func CloneState(state State) State {
	devices := make(map[string]Device, len(state.Devices))
	for id, device := range state.Devices {
		devices[id] = device
	}
	return State{Devices: devices}
}

func Apply(state *State, event replay.RecordedEvent) error {
	switch event.Type {
	case EventRegistered:
		payload, err := replay.DecodeEvent[Registered](event)
		if err != nil {
			return err
		}
		if _, exists := state.Devices[payload.DeviceID]; exists {
			return fmt.Errorf("device %q already registered", payload.DeviceID)
		}
		state.Devices[payload.DeviceID] = Device{ID: payload.DeviceID, Model: payload.Model}
	case EventStatusReported:
		payload, err := replay.DecodeEvent[StatusReported](event)
		if err != nil {
			return err
		}
		device, exists := state.Devices[payload.DeviceID]
		if !exists {
			return fmt.Errorf("device %q is not registered", payload.DeviceID)
		}
		device.Online = payload.Online
		device.Signal = payload.SignalDBM
		state.Devices[payload.DeviceID] = device
	case EventFirmwareUpdated:
		payload, err := replay.DecodeEvent[FirmwareUpdated](event)
		if err != nil {
			return err
		}
		device, exists := state.Devices[payload.DeviceID]
		if !exists {
			return fmt.Errorf("device %q is not registered", payload.DeviceID)
		}
		if payload.Firmware == "" {
			return fmt.Errorf("device %q firmware is empty", payload.DeviceID)
		}
		device.Firmware = payload.Firmware
		state.Devices[payload.DeviceID] = device
	default:
		return fmt.Errorf("unknown event type %q", event.Type)
	}
	return nil
}
