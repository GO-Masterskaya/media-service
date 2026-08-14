// v1 — ВРЕМЕННАЯ ЗАГЛУШКА для компиляции задачи #12.
// Полная реализация ожидается в #4. После реализации этот файл можно удалить, но сигнатуры методов должны быть совместимы.
package v1

import "context"

// MediaServiceServer — интерфейс gRPC-сервиса (SPEC §3).
type MediaServiceServer interface {
	DownloadStream(*DownloadStreamRequest, MediaService_DownloadStreamServer) error
}

// UnimplementedMediaServiceServer — заглушка для встраивания.
type UnimplementedMediaServiceServer struct{}

func (UnimplementedMediaServiceServer) DownloadStream(*DownloadStreamRequest, MediaService_DownloadStreamServer) error {
	return nil
}

// DownloadStreamRequest — запрос на потоковое скачивание.
type DownloadStreamRequest struct {
	MediaId string
	Variant string
}

// DownloadChunk — один чанк ответа.
type DownloadChunk struct {
	Data []byte
}

// MediaService_DownloadStreamServer — интерфейс server-streaming вызова.
type MediaService_DownloadStreamServer interface {
	Context() context.Context
	Send(*DownloadChunk) error
}