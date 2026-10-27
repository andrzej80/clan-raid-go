package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// connPool — n независимых ClientConn (каждое со своим TCP-соединением), по которым запросы
// раздаются по кругу. Реализует grpc.ClientConnInterface, поэтому подставляется вместо
// одного *grpc.ClientConn. Один стрим или unary-вызов целиком живёт на одном соединении.
type connPool struct {
	conns []*grpc.ClientConn
	next  atomic.Uint64
}

var _ grpc.ClientConnInterface = (*connPool)(nil)

func newConnPool(addr string, n int) (*connPool, error) {
	if n <= 0 {
		return nil, fmt.Errorf("conns must be positive, got %d", n)
	}
	p := &connPool{conns: make([]*grpc.ClientConn, 0, n)}
	for range n {
		c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			_ = p.Close() //nolint:gosec // уже возвращаем исходную ошибку
			return nil, fmt.Errorf("grpc client: %w", err)
		}
		p.conns = append(p.conns, c)
	}
	return p, nil
}

func (p *connPool) pick() *grpc.ClientConn {
	return p.conns[(p.next.Add(1)-1)%uint64(len(p.conns))]
}

func (p *connPool) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	return p.pick().Invoke(ctx, method, args, reply, opts...)
}

func (p *connPool) NewStream(
	ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	return p.pick().NewStream(ctx, desc, method, opts...)
}

// Close закрывает все соединения и возвращает объединённую ошибку.
func (p *connPool) Close() error {
	errs := make([]error, 0, len(p.conns))
	for _, c := range p.conns {
		errs = append(errs, c.Close())
	}
	return errors.Join(errs...)
}
