package interceptors

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const callerIDKey = "x-caller-id"

// Caller отвечает на вопрос: "Кто вызывает RPC?"
//
//	(В него происходит извлечение идентификатора вызывающего)
type Caller struct {
	ID string
}

type callerKey struct{}

type callerServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func WithCaller(ctx context.Context, caller Caller) context.Context {
	return context.WithValue(ctx, callerKey{}, caller)
}

func CallerFromContext(ctx context.Context) (Caller, bool) {
	caller, ok := ctx.Value(callerKey{}).(Caller)
	return caller, ok
	// true= Caller найден и имеет тип Caller
}

func CallerUnaryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	// этот параметр- функция, которая должна обработать настоящий RPC
	handler grpc.UnaryHandler,
) (any, error) {

	// берём исходный context запроса и пробуем достать caller из его metadata
	caller, err := callerFromMetadata(ctx)
	if err != nil {
		// если caller получить не удалось, запрос дальше не отправляем. Клиенту возвращаем ошибку.
		return nil, err
	}

	// создаём на основе context текущего запроса новый, кладём туда caller
	ctx = WithCaller(ctx, caller)

	//теперь передаём новый context и request настоящему обработчику RPC
	return handler(ctx, req)
}

// нужна, чтобы не засорять основной interceptor
func callerFromMetadata(ctx context.Context) (Caller, error) {
	// берём metadata из context входящего gRPC-запроса
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		// если metadata не, возвращаем пустой Caller и gRPC-ошибку
		return Caller{}, status.Error(codes.Unauthenticated, "caller identity required")
	}

	// берём из metadata все значения с именем "x-caller-id"
	values := md.Get(callerIDKey)
	// если значений нет, или первое значение пустое, caller отсутствует
	if len(values) == 0 || values[0] == "" {
		return Caller{}, status.Error(codes.Unauthenticated, "caller identity required")
	}

	// если ок, то создаём Caller и кладём туда первое значение x-caller-id
	return Caller{
		ID: values[0],
	}, nil
}

// этот код реализует только unary RPC, теперь нужно добавить CallerStreamInterceptor
func CallerStreamInterceptor(
	srv any,
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	caller, err := callerFromMetadata(ss.Context())
	if err != nil {
		return err
	}

	// context находится внутри ss, поэтому создаём новый context
	ctx := WithCaller(ss.Context(), caller)

	// и новую обёртку вокруг ss
	wrapped := &callerServerStream{
		ServerStream: ss,
		ctx:          ctx,
	}

	return handler(srv, wrapped)
}

// и переопределяем
func (s *callerServerStream) Context() context.Context {
	return s.ctx
}
