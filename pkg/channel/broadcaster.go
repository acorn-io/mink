package channel

import (
	"context"
	"sync"
)

type Broadcaster[T any] struct {
	lock      sync.Mutex
	consumers map[*Subscription[T]]struct{}
	C         chan T
	closed    bool
}

func NewBroadcaster[T any](c chan T) *Broadcaster[T] {
	return &Broadcaster[T]{
		consumers: map[*Subscription[T]]struct{}{},
		C:         c,
	}
}

func (b *Broadcaster[T]) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			b.Close()
			return
		case i, ok := <-b.C:
			if !ok {
				return
			}
			b.lock.Lock()
			for sub := range b.consumers {
				sub.C <- i
			}
			b.lock.Unlock()
		}
	}
}

func (b *Broadcaster[T]) Close() {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.closed {
		return
	}
	for sub := range b.consumers {
		sub.close(false)
	}
	b.closed = true
	close(b.C)
}

func (b *Broadcaster[T]) Subscribe() *Subscription[T] {
	b.lock.Lock()
	defer b.lock.Unlock()
	c := make(chan T, 1)
	if b.closed {
		close(c)
		return &Subscription[T]{
			C:           c,
			broadcaster: b,
			closed:      true,
		}
	}
	sub := &Subscription[T]{
		C:           c,
		broadcaster: b,
	}
	b.consumers[sub] = struct{}{}
	return sub
}

type Subscription[T any] struct {
	C           chan T
	broadcaster *Broadcaster[T]
	closed      bool
}

func (s *Subscription[T]) Close() {
	s.close(true)
}

func (s *Subscription[T]) close(lock bool) {
	if lock {
		s.broadcaster.lock.Lock()
		defer s.broadcaster.lock.Unlock()
	}
	if s.closed {
		return
	}
	delete(s.broadcaster.consumers, s)
	close(s.C)
	s.closed = true
}
