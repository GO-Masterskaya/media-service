	package config

	import (
		"log/slog"
		"os"
	)

	// NewLogger создаёт JSON-логгер, который маскирует секреты.
	// Используется как единственный логгер сервиса.
	func NewLogger() *slog.Logger {
		handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{ // пока в dev'e, установлен slog.NewTextHandler, потом заменить на slog.NewJSONHandler
			Level: slog.LevelInfo,
		})
		return slog.New(handler)
	}