// надо проверить
// request counter увеличивается
//code записывается
// duration появляется
// active streams +1, после завершения -1

// удалит потом это всё
package metrics

import (
	"context"

	"google.golang.org/grpc/metadata"
)

type testServerStream struct {
	ctx context.Context
}

func (s *testServerStream) Context() context.Context {
	return s.ctx
}

func (s *testServerStream) SetHeader(metadata.MD) error {
	return nil
}

func (s *testServerStream) SendHeader(metadata.MD) error {
	return nil
}

func (s *testServerStream) SetTrailer(metadata.MD) {}

func (s *testServerStream) SendMsg(any) error {
	return nil
}

func (s *testServerStream) RecvMsg(any) error {
	return nil
}
