// для bounded cleanup состояния неактивных callers
// здесь будет жить map[string]*CallerState
package interceptors

import (
	"context"
	"time"
)

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
		// сколько времени прошло с последнего использования этого caller?
		if now.Sub(entry.lastUsed) >= inactiveAfter {
			// если прошло больше inactiveAfter, то запись удаляется
			delete(l.limiters, callerID)
		}
	}
}
