package device

import (
	"fmt"

	"replayable-app"
)

const (
	EventRegistered      = "device.registered"
	EventOnlineChanged   = "device.online_changed"
	EventReported        = "device.reported"
	EventSettingsApplied = "device.settings_applied"
)

type Registered struct {
	Name string `json:"name"`
}

type OnlineChanged struct {
	Online bool `json:"online"`
}

type Reported struct {
	Reading int `json:"reading"`
}

type SettingsApplied struct {
	Mode     string `json:"mode"`
	Interval int    `json:"interval_seconds"`
}

type Settings struct {
	Mode            string `json:"mode"`
	IntervalSeconds int    `json:"interval_seconds"`
}

type State struct {
	DeviceID      string   `json:"device_id"`
	Name          string   `json:"name"`
	Online        bool     `json:"online"`
	LatestReading int      `json:"latest_reading"`
	ReportCount   int      `json:"report_count"`
	Settings      Settings `json:"settings"`
}

type FleetState map[string]State

func NewState() State {
	return State{Settings: Settings{Mode: "default", IntervalSeconds: 60}}
}

func CloneState(state State) State {
	return state
}

func NewFleetState() FleetState {
	return make(FleetState)
}

func CloneFleetState(state FleetState) FleetState {
	cloned := make(FleetState, len(state))
	for deviceID, device := range state {
		cloned[deviceID] = device
	}
	return cloned
}

func Apply(state *State, event replay.RecordedEvent) error {
	switch event.Type {
	case EventRegistered:
		payload, err := replay.DecodeEvent[Registered](event)
		if err != nil {
			return err
		}
		if state.DeviceID != "" {
			return fmt.Errorf("device already registered")
		}
		state.DeviceID = event.StreamID
		state.Name = payload.Name
	case EventOnlineChanged:
		payload, err := replay.DecodeEvent[OnlineChanged](event)
		if err != nil {
			return err
		}
		if state.DeviceID == "" {
			return fmt.Errorf("device is not registered")
		}
		state.Online = payload.Online
	case EventReported:
		payload, err := replay.DecodeEvent[Reported](event)
		if err != nil {
			return err
		}
		if !state.Online {
			return fmt.Errorf("device is offline")
		}
		state.LatestReading = payload.Reading
		state.ReportCount++
	case EventSettingsApplied:
		payload, err := replay.DecodeEvent[SettingsApplied](event)
		if err != nil {
			return err
		}
		if payload.Interval <= 0 {
			return fmt.Errorf("settings interval must be positive")
		}
		state.Settings = Settings{Mode: payload.Mode, IntervalSeconds: payload.Interval}
	default:
		return fmt.Errorf("unknown event type %q", event.Type)
	}
	return nil
}
