package processing_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"mediaservice/internal/processing"
)

// Тест проверяет, что RepoAdapter реализует интерфейс processing.JobRepository.
func TestRepoAdapterImplementsInterface(t *testing.T) {
	var _ processing.JobRepository = (*processing.RepoAdapter)(nil)
}

// Тест проверяет валидацию UUID в FailJob адаптера
func TestRepoAdapterFailJobInvalidUUID(t *testing.T) {
	adapter := processing.NewRepoAdapter(nil, "worker-test")
	err := adapter.FailJob(context.Background(), "invalid-uuid", "some reason")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parse job id")
}
