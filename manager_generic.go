package replay

func RegisterProjection[S State](manager *Manager, name string, state S, projection *Projection[S]) {
	manager.Register(name, state, func(current State, event Event) (State, error) {
		return projection.Apply(current.(S), event)
	})
}
