package mq

import (
	"context"
)

type Publisher[E any] interface {
	Publish(messages E) error
	Close() error
}

type Subscriber[M any] interface {
	Subscribe(ctx context.Context, bufferSize uint) (<-chan Message[M], error)
	Close() error
}
