package interceptors

import (
	"context"
	"time"
)

// StartRateLimiterCleanup периодически удаляет состояние callers,
// которые не использовали limiter дольше inactiveAfter.
//
// Функция блокирующая. При запуске сервиса её нужно вызывать
// в отдельной горутине.
func StartRateLimiterCleanup(
	ctx context.Context,
	limiter *RateLimiter,
	interval time.Duration,
	inactiveAfter time.Duration,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			limiter.cleanup(inactiveAfter)
		}
	}
}

func (l *RateLimiter) cleanup(inactiveAfter time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()

	for callerID, entry := range l.limiters {
		if now.Sub(entry.lastUsed) >= inactiveAfter {
			delete(l.limiters, callerID)
		}
	}
}
