package eventio

import (
	"context"
	"errors"
	"sync"
)

type MessageQueue[M any] struct {
	subscribers     []*subscriber[M]
	subscribersLock sync.RWMutex
	subscribersWg   sync.WaitGroup

	closing    chan struct{}
	closed     bool
	closedLock sync.Mutex
}

func NewQueue[M any]() *MessageQueue[M] {
	return &MessageQueue[M]{
		subscribers:     make([]*subscriber[M], 0),
		subscribersLock: sync.RWMutex{},
		subscribersWg:   sync.WaitGroup{},
		closing:         make(chan struct{}),
		closed:          false,
		closedLock:      sync.Mutex{},
	}
}

func (m *MessageQueue[M]) Publish(msg M) error {
	if m.isClosed() {
		return errors.New("Pub/Sub closed")
	}

	m.subscribersLock.RLock()
	defer m.subscribersLock.RUnlock()

	m.sendMessage(&message[M]{payload: msg})
	return nil
}

func (m *MessageQueue[M]) sendMessage(msg *message[M]) {
	for i := range m.subscribers {
		subscriber := m.subscribers[i]
		go func() {
			subscriber.sendMessageToMessageChannel(msg)
		}()
	}
}

func (m *MessageQueue[M]) Subscribe(ctx context.Context, bufferSize uint) (<-chan Message[M], error) {
	m.closedLock.Lock()

	if m.closed {
		m.closedLock.Unlock()
		return nil, errors.New("Pub/Sub closed")
	}

	m.subscribersWg.Add(1)
	m.closedLock.Unlock()

	m.subscribersLock.Lock()

	s := &subscriber[M]{
		ctx:           ctx,
		messageCh:     make(chan Message[M], bufferSize),
		messageChLock: sync.Mutex{},

		closed:  false,
		closing: make(chan struct{}),
	}

	go func(s *subscriber[M], g *MessageQueue[M]) {
		select {
		case <-ctx.Done():
		case <-g.closing:
		}
		s.Close()

		g.subscribersLock.Lock()
		defer g.subscribersLock.Unlock()

		g.removeSubscriber(s)
		g.subscribersWg.Done()
	}(s, m)

	go func(s *subscriber[M]) {
		defer m.subscribersLock.Unlock()

		m.addSubscriber(s)
	}(s)

	return s.messageCh, nil
}

func (m *MessageQueue[M]) addSubscriber(s *subscriber[M]) {
	m.subscribers = append(m.subscribers, s)
}

func (m *MessageQueue[M]) removeSubscriber(toRemove *subscriber[M]) {
	for i, sub := range m.subscribers {
		if sub == toRemove {
			m.subscribers = append(m.subscribers[:i], m.subscribers[i+1:]...)
			break
		}
	}
}

func (m *MessageQueue[M]) Close() error {

	m.closedLock.Lock()
	defer m.closedLock.Unlock()

	m.closed = true
	close(m.closing)

	m.subscribersWg.Wait()
	return nil
}

func (m *MessageQueue[M]) isClosed() bool {
	m.closedLock.Lock()
	defer m.closedLock.Unlock()

	return m.closed
}

type subscriber[M any] struct {
	ctx context.Context

	messageCh     chan Message[M]
	messageChLock sync.Mutex

	closing chan struct{}
	closed  bool
}

func (s *subscriber[M]) Close() {
	if s.closed {
		return
	}
	close(s.closing)

	s.messageChLock.Lock()
	defer s.messageChLock.Unlock()

	s.closed = true

	close(s.messageCh)
}

func (s *subscriber[M]) sendMessageToMessageChannel(msg *message[M]) {
	s.messageChLock.Lock()
	defer s.messageChLock.Unlock()

ReTrySendMessageToMessageChannel:
	for {
		if s.closed {
			return
		}
		msg = msg.Copy()
		select {
		case s.messageCh <- msg:
		case <-s.closing:
			return
		}

		select {
		case <-msg.ack:
			if msg.err != nil {
				continue ReTrySendMessageToMessageChannel
			}
			return
		case <-s.closing:
			return
		}
	}
}
