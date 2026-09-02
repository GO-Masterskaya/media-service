package repo

import (
	"math/rand/v2"
	"time"
)

// JobBackoffConfig задаёт exponential backoff с jitter для run_after.
type JobBackoffConfig struct {
	Base   time.Duration // начальная задержка (после 1-й неудачи)
	Max    time.Duration // потолок задержки (после jitter не превышается)
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

// Normalize возвращает копию с безопасными границами Base/Max/Jitter.
func (c JobBackoffConfig) Normalize() JobBackoffConfig {
	if c.Base <= 0 {
		c.Base = 30 * time.Second
	}
	if c.Max <= 0 {
		c.Max = 10 * time.Minute
	}
	if c.Base > c.Max {
		c.Base = c.Max
	}
	if c.Jitter < 0 {
		c.Jitter = 0
	}
	if c.Jitter > 1 {
		c.Jitter = 1
	}
	return c
}

// Delay возвращает задержку до следующей попытки (≤ Max).
// attemptsAfterIncrement — значение attempts после инкремента (1 = первая повторная попытка).
func (c JobBackoffConfig) Delay(attemptsAfterIncrement int) time.Duration {
	c = c.Normalize()
	if attemptsAfterIncrement <= 0 {
		attemptsAfterIncrement = 1
	}

	delay := c.Base
	for i := 1; i < attemptsAfterIncrement; i++ {
		if delay >= c.Max {
			delay = c.Max
			break
		}
		next := delay * 2
		if next > c.Max {
			delay = c.Max
		} else {
			delay = next
		}
	}

	delay = applyJitter(delay, c.Jitter)
	if delay > c.Max {
		delay = c.Max
	}
	if delay < 0 {
		delay = 0
	}
	return delay
}

// NextRunAfter вычисляет run_after для attempts после инкремента.
func (c JobBackoffConfig) NextRunAfter(attemptsAfterIncrement int) time.Time {
	return time.Now().Add(c.Delay(attemptsAfterIncrement))
}

func applyJitter(d time.Duration, jitter float64) time.Duration {
	if jitter <= 0 || d <= 0 {
		return d
	}
	if jitter > 1 {
		jitter = 1
	}
	// uniform in [d*(1-jitter), d*(1+jitter)], затем вызывающий клампит к Max.
	factor := 1 - jitter + rand.Float64()*(2*jitter)
	return time.Duration(float64(d) * factor)
}
