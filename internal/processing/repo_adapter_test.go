package processing_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"mediaservice/internal/processing"
)

func testBackoff() processing.BackoffConfig {
	return processing.BackoffConfig{
		Base:   30 * time.Second,
		Max:    10 * time.Minute,
		Jitter: 0,
	}
}

// Тест проверяет, что RepoAdapter реализует интерфейс processing.JobRepository.
func TestRepoAdapterImplementsInterface(t *testing.T) {
	var _ processing.JobRepository = (*processing.RepoAdapter)(nil)
}

// Тест проверяет валидацию UUID в FailJob адаптера
func TestRepoAdapterFailJobInvalidUUID(t *testing.T) {
	adapter := processing.NewRepoAdapter(nil, "worker-test", 30*time.Second, 3, testBackoff())
	err := adapter.FailJob(context.Background(), "invalid-uuid", "some reason")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parse job id")
}

// Тест проверяет валидацию UUID в MarkDone адаптера
func TestRepoAdapterMarkDoneInvalidUUID(t *testing.T) {
	adapter := processing.NewRepoAdapter(nil, "worker-test", 30*time.Second, 3, testBackoff())
	err := adapter.MarkDone(context.Background(), "invalid-uuid")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parse job id")
}

// Тест проверяет валидацию UUID в ReleaseJobForRetry адаптера
func TestRepoAdapterReleaseJobInvalidUUID(t *testing.T) {
	adapter := processing.NewRepoAdapter(nil, "worker-test", 30*time.Second, 3, testBackoff())
	err := adapter.ReleaseJobForRetry(context.Background(), "invalid-uuid", 1, "reason")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parse job id")
}

// Тест проверяет валидацию UUID в ExtendLease адаптера
func TestRepoAdapterExtendLeaseInvalidUUID(t *testing.T) {
	adapter := processing.NewRepoAdapter(nil, "worker-test", 30*time.Second, 3, testBackoff())
	err := adapter.ExtendLease(context.Background(), "invalid-uuid", 30*time.Second)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parse job id")
}
