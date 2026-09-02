package repo

import (
	"math/rand/v2"
	"time"
)

// JobBackoffConfig задаёт exponential backoff с jitter для run_after.
type JobBackoffConfig struct {
	Base   time.Duration // начальная задержка (после 1-й неудачи)
	Max    time.Duration // потолок задержки
	Jitter float64       // доля jitter [0..1], например 0.2 = ±20%
}

// DefaultJobBackoff — дефолты из SPEC/TZ (base 30s).
func DefaultJobBackoff() JobBackoffConfig {
	return JobBackoffConfig{
		Base:   30 * time.Second,
		Max:    10 * time.Minute,
		Jitter: 0.2,
	}
}

// Delay возвращает задержку до следующей попытки.
// attemptsAfterIncrement — значение attempts после инкремента (1 = первая повторная попытка).
func (c JobBackoffConfig) Delay(attemptsAfterIncrement int) time.Duration {
	if attemptsAfterIncrement <= 0 {
		attemptsAfterIncrement = 1
	}
	base := c.Base
	if base <= 0 {
		base = 30 * time.Second
	}
	max := c.Max
	if max <= 0 {
		max = 10 * time.Minute
	}

	delay := base
	for i := 1; i < attemptsAfterIncrement; i++ {
		if delay >= max {
			return applyJitter(max, c.Jitter)
		}
		next := delay * 2
		if next > max {
			delay = max
		} else {
			delay = next
		}
	}
	return applyJitter(delay, c.Jitter)
}

// NextRunAfter вычисляет run_after для attempts после инкремента.
func (c JobBackoffConfig) NextRunAfter(attemptsAfterIncrement int) time.Time {
	return time.Now().Add(c.Delay(attemptsAfterIncrement))
}

func applyJitter(d time.Duration, jitter float64) time.Duration {
	if jitter <= 0 {
		return d
	}
	if jitter > 1 {
		jitter = 1
	}
	// uniform in [d*(1-jitter), d*(1+jitter)]
	factor := 1 - jitter + rand.Float64()*(2*jitter)
	return time.Duration(float64(d) * factor)
}
