package eventio

type Message[M any] interface {
	Payload() M
	Ack()
	Err(err error)
}

type message[M any] struct {
	payload M
	err     error
	ack     chan struct{}
	acked   bool
}

func (m *message[M]) Payload() M {
	return m.payload
}

func (m *message[M]) Ack() {
	if !m.acked {
		close(m.ack)
	}
	m.acked = true
}

func (m *message[M]) Err(err error) {
	m.err = err
	m.Ack()
}

func (m *message[M]) Copy() *message[M] {
	return &message[M]{
		payload: m.payload,
		err:     nil,
		ack:     make(chan struct{}),
		acked:   false,
	}
}
