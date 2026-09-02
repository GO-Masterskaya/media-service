package processing

import (
	"fmt"
	"sync"
)

// Registry хранит сопоставление между типом задачи (jobType) и соответствующим обработчиком.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry создаёт новый пустой реестр обработчиков.
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]Handler),
	}
}

// Register регистрирует обработчик для конкретного типа задач.
// Паникует при попытке зарегистрировать дублирующий тип — это ошибка программиста,
// которую нужно ловить на этапе запуска, а не в runtime.
func (r *Registry) Register(jobType string, handler Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[jobType]; exists {
		panic(fmt.Sprintf("processing: handler for job type %q already registered", jobType))
	}
	r.handlers[jobType] = handler
}

// Get возвращает зарегистрированный обработчик по типу задачи.
// Если обработчик не найден, возвращает ErrUnknownJobType.
func (r *Registry) Get(jobType string) (Handler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	h, ok := r.handlers[jobType]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownJobType, jobType)
	}
	return h, nil
}

// Len возвращает количество зарегистрированных обработчиков.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.handlers)
}
