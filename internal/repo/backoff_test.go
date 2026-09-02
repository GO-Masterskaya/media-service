package repo

import (
	"testing"
	"time"
)

func TestJobBackoffConfig_Delay(t *testing.T) {
	cfg := JobBackoffConfig{Base: 30 * time.Second, Max: 5 * time.Minute, Jitter: 0}

	tests := []struct {
		attempts int
		want     time.Duration
	}{
		{1, 30 * time.Second},
		{2, 60 * time.Second},
		{3, 2 * time.Minute},
		{4, 4 * time.Minute},
		{5, 5 * time.Minute}, // capped at max
		{6, 5 * time.Minute},
	}
	for _, tt := range tests {
		if got := cfg.Delay(tt.attempts); got != tt.want {
			t.Errorf("Delay(%d) = %s, want %s", tt.attempts, got, tt.want)
		}
	}
}

func TestJobBackoffConfig_JitterWithinBounds(t *testing.T) {
	cfg := JobBackoffConfig{Base: 30 * time.Second, Max: 10 * time.Minute, Jitter: 0.2}
	base := 30 * time.Second
	min := time.Duration(float64(base) * 0.8)
	max := time.Duration(float64(base) * 1.2)

	for range 100 {
		got := cfg.Delay(1)
		if got < min || got > max {
			t.Fatalf("jittered delay %s outside [%s, %s]", got, min, max)
		}
	}
}
