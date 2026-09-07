package mediaservice_test

import (
	"context"
	"testing"

	"mediaservice/internal/media"
	"mediaservice/internal/repo"
	"mediaservice/pkg/mediaservice"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"
)

// TestErrors_NotFound - несуществующий объект даёт публичный ErrNotFound.
func (s *ClientSuite) TestErrors_NotFound() {
	t := s.T()
	client := s.newClient()
	defer func() { _ = client.Close() }()

	_, err := client.GetMedia(s.ctx, uuid.New(), uuid.New())
	require.ErrorIs(t, err, mediaservice.ErrNotFound)
}

// TestErrors_InternalTypesDoNotLeak - обратная половина критерия.
//
// Тест намеренно импортирует internal-пакеты, чего настоящий потребитель
// сделать не смог бы. Именно поэтому проверку нельзя написать снаружи,
// и именно поэтому её легко забыть: утечка внутреннего типа никак
// не проявляется, пока кто-нибудь на неё не наткнётся.
func (s *ClientSuite) TestErrors_InternalTypesDoNotLeak() {
	t := s.T()
	client := s.newClient()
	defer func() { _ = client.Close() }()

	_, err := client.GetMedia(s.ctx, uuid.New(), uuid.New())
	require.Error(t, err)

	require.NotErrorIs(t, err, repo.ErrNotFound,
		"наружу вышел сентинел пакета repo")
	require.NotErrorIs(t, err, media.ErrNotFound,
		"наружу вышел сентинел пакета media")

	_, ok := status.FromError(err)
	require.False(t, ok,
		"ошибка разбирается как gRPC-статус: транспортный тип протёк наружу")
}

// TestErrors_InvalidArgument - валидация входа до похода в базу.
func (s *ClientSuite) TestErrors_InvalidArgument() {
	t := s.T()
	client := s.newClient()
	defer func() { _ = client.Close() }()

	mediaID := uuid.New()
	ownerID := uuid.New()

	cases := []struct {
		name string
		call func() error
	}{
		{"нулевой ownerID", func() error {
			_, err := client.GetMedia(s.ctx, uuid.Nil, mediaID)
			return err
		}},
		{"нулевой mediaID", func() error {
			_, err := client.GetMedia(s.ctx, ownerID, uuid.Nil)
			return err
		}},
		{"неизвестный вариант", func() error {
			_, err := client.GetDownloadURL(s.ctx, ownerID, mediaID,
				mediaservice.Variant("no-such-variant"))
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorIs(t, tc.call(), mediaservice.ErrInvalidArgument)
		})
	}
}

// TestErrors_AccessDenied - чужой объект.
//
// Запись вставляется напрямую в базу: Upload пока заглушка (#9), а без
// существующего объекта проверить проверку владельца нечем.
func (s *ClientSuite) TestErrors_AccessDenied() {
	t := s.T()
	client := s.newClient()
	defer func() { _ = client.Close() }()

	owner := uuid.New()
	mediaID := s.insertMedia(owner, "stored")

	_, err := client.GetMedia(s.ctx, uuid.New(), mediaID)
	require.ErrorIs(t, err, mediaservice.ErrAccessDenied)

	// Владелец при этом объект видит - иначе тест доказывал бы лишь то,
	// что метод всегда отказывает.
	got, err := client.GetMedia(s.ctx, owner, mediaID)
	require.NoError(t, err)
	require.Equal(t, mediaID, got.ID)
}

// TestErrors_NotReady - операция невозможна в текущем статусе объекта.
func (s *ClientSuite) TestErrors_NotReady() {
	t := s.T()
	client := s.newClient()
	defer func() { _ = client.Close() }()

	owner := uuid.New()

	// Ссылку на оригинал не дают для failed.
	failed := s.insertMedia(owner, "failed")
	_, err := client.GetDownloadURL(s.ctx, owner, failed, mediaservice.VariantOriginal)
	require.ErrorIs(t, err, mediaservice.ErrNotReady)

	// Производную не дают, пока объект не ready.
	stored := s.insertMedia(owner, "stored")
	_, err = client.GetDownloadURL(s.ctx, owner, stored, mediaservice.VariantThumb)
	require.ErrorIs(t, err, mediaservice.ErrNotReady)

	// Удалять объект в обработке нельзя.
	processing := s.insertMedia(owner, "processing")
	require.ErrorIs(t, client.Delete(s.ctx, owner, processing), mediaservice.ErrNotReady)
}

// TestErrors_ContextCanceled - отмена возвращается как стандартный сентинел,
// а не склеивается в ErrInternal.
//
// Проверяется на DeleteByOwner и DownloadStream: только они доносят отмену
// до библиотеки. GetMedia, GetDownloadURL и DeleteMedia конвертируют любую
// не-NotFound ошибку в status.Error(codes.Internal), а status.Error создаёт
// новую ошибку вместо обёртки - цепочка рвётся, и context.Canceled внутри
// ядра теряется. Это ограничение ядра, а не библиотеки.
func (s *ClientSuite) TestErrors_ContextCanceled() {
	t := s.T()
	client := s.newClient()
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithCancel(s.ctx)
	cancel()

	// DeleteByOwner возвращает ctx.Err() напрямую, ещё до похода в базу.
	_, err := client.DeleteByOwner(ctx, uuid.New())
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, mediaservice.ErrInternal,
		"отмена вызывающего выдана за внутреннюю ошибку сервиса")

	// DownloadStream идёт через слой репозитория и оборачивает ошибку
	// через %w, поэтому отмена доезжает наружу из глубины.
	_, err = client.DownloadStream(ctx, uuid.New(), uuid.New(),
		mediaservice.VariantOriginal)
	require.ErrorIs(t, err, context.Canceled)
}

// --- вспомогательное ---

// newClient создаёт клиента на внешних ресурсах сюиты.
func (s *ClientSuite) newClient() *mediaservice.Client {
	s.T().Helper()

	pool := s.newExternalPool()
	// Пул создан тестом, значит закрывать его тоже тесту: клиент внешними
	// ресурсами не владеет. Cleanup сработает по завершении теста,
	// в том числе при падении.
	s.T().Cleanup(pool.Close)

	client, err := mediaservice.NewWithDeps(s.ctx, mediaservice.Deps{
		Pool:   pool,
		MinIO:  s.newExternalMinIO(),
		Bucket: testBucket,
	})
	require.NoError(s.T(), err)
	return client
}

// insertMedia кладёт запись напрямую в базу и возвращает её идентификатор.
//
// Перечислены только колонки без DEFAULT: остальные база заполнит сама.
// Ключ идемпотентности случайный - на паре (owner_id, idempotency_key)
// стоит уникальный индекс.
func (s *ClientSuite) insertMedia(ownerID uuid.UUID, status string) uuid.UUID {
	s.T().Helper()

	id := uuid.New()
	_, err := s.admin.Exec(s.ctx, `
		INSERT INTO media
			(id, owner_id, kind, orig_filename, mime, size_bytes,
			 status, storage_key, idempotency_key)
		VALUES ($1, $2, 'image', 'test.jpg', 'image/jpeg', 1024,
			 $3, $4, $5)`,
		id, ownerID, status, "media/"+id.String()+"/original", uuid.NewString())
	require.NoError(s.T(), err)

	return id
}
