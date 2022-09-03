package eventkit

type Sender interface {
	QueueSend(EventMap)
}

type Registry struct {
	senders []Sender
}

func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) Scope(name string) *Scope {
	return &Scope{
		r:    r,
		name: []string{name},
	}
}

// AddOutput adds an output to the registry. Do not call AddOutput if
// Submit might be called concurrently. It is expected that AddOutput
// will be called at initialization time before any events.
func (r *Registry) AddOutput(sender Sender) {
	r.senders = append(r.senders, sender)
}

type EventMap map[string]interface{}

// Submit takes an EventMap. It is likely that all output destinations
// will drop events that don't have "name", "scope", or "timestamp" fields
// specified. Using a *Scope's Event helper will help you get this right.
func (r *Registry) Submit(em EventMap) {
	for _, sender := range r.senders {
		sender.QueueSend(em)
	}
}
