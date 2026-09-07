// знает caller
// считает активные стримы
package interceptors

import (
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type StreamLimiter struct {
	mu     sync.Mutex
	active map[string]int
	max    int
}

func NewStreamLimiter(max int) *StreamLimiter {
	return &StreamLimiter{
		active: make(map[string]int),
		max:    max,
	}
}

func (l *StreamLimiter) acquire(callerID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.active[callerID] >= l.max {
		return false
	}

	l.active[callerID]++

	return true
}

func (l *StreamLimiter) release(callerID string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.active[callerID]--

	if l.active[callerID] <= 0 {
		delete(l.active, callerID)
	}
}

func (l *StreamLimiter) Interceptor(
	srv any,
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	caller, ok := CallerFromContext(ss.Context())
	if !ok {
		return status.Error(codes.Unauthenticated, "caller identity is required")
	}

	if !l.acquire(caller.ID) {
		return status.Error(
			codes.ResourceExhausted,
			"too many active streams",
		)
	}
	//когда этот поток закончится по любой причине, освобождаем занятый слот
	defer l.release(caller.ID)

	return handler(srv, ss)
}
