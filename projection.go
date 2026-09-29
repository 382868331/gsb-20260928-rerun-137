package replay

import "fmt"

type State interface {
	Version() int64
}

type Handler[S State] func(state S, event Event, payload any) (S, error)

type Projection[S State] struct {
	name     string
	codec    *Codec
	handlers map[string]Handler[S]
}

func NewProjection[S State](name string, codec *Codec) *Projection[S] {
	return &Projection[S]{name: name, codec: codec, handlers: make(map[string]Handler[S])}
}

func (p *Projection[S]) Name() string { return p.name }

func (p *Projection[S]) On(eventType string, handler Handler[S]) {
	p.handlers[eventType] = handler
}

func (p *Projection[S]) Apply(state S, event Event) (S, error) {
	payload, err := p.codec.Decode(event)
	if err != nil {
		return state, err
	}
	handler, ok := p.handlers[event.Type]
	if !ok {
		return state, fmt.Errorf("projection %q has no handler for event %q", p.name, event.Type)
	}
	next, err := handler(state, event, payload)
	if err != nil {
		return state, fmt.Errorf("projection %q failed at sequence %d event %q: %w", p.name, event.Sequence, event.Type, err)
	}
	if next.Version() != event.Sequence {
		return state, fmt.Errorf("projection %q returned version %d after sequence %d", p.name, next.Version(), event.Sequence)
	}
	return next, nil
}
