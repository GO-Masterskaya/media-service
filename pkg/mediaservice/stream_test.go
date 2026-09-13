package mediaservice_test

import (
	"io"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"mediaservice/pkg/mediaservice"
)

// TestDownloadStream_ErrClosedAfterClientClose - чтение после закрытия
// клиента даёт ErrClosed.
//
// Запись кладётся напрямую в базу, и объекта в хранилище за ней нет.
// Так и задумано: тест проверяет, что обёртка отказывает **до** обращения
// к нижележащему потоку. Если бы проверка стояла после, мы получили бы
// ошибку MinIO про отсутствующий ключ, и тест бы не различил эти случаи.
//
// Зачем это вообще нужно. DownloadStream отпускает блокировку клиента
// сразу после возврата - держать её всё время чтения нельзя, иначе один
// медленный читатель подвесил бы Close для всех. Значит клиент может быть
// закрыт, пока поток ещё читают, и вызывающий должен получить понятную
// ошибку, а не ошибку уровня драйвера.
func (s *ClientSuite) TestDownloadStream_ErrClosedAfterClientClose() {
	t := s.T()
	client := s.newClient()

	owner := uuid.New()
	id := s.insertMedia(owner, "stored")

	stream, err := client.DownloadStream(s.ctx, owner, id, mediaservice.VariantOriginal)
	require.NoError(t, err)
	defer func() { _ = stream.Close() }()

	require.NoError(t, client.Close())

	buf := make([]byte, 32)
	n, err := stream.Read(buf)
	require.Zero(t, n)
	require.ErrorIs(t, err, mediaservice.ErrClosed)
}

// TestDownloadStream_CloseWorksAfterClientClose - закрыть поток можно
// и после закрытия клиента.
//
// Соблазн отдавать ErrClosed и здесь есть, но это превратило бы страховку
// в утечку: соединение с хранилищем нужно освободить в любом случае,
// а больше его закрыть некому.
func (s *ClientSuite) TestDownloadStream_CloseWorksAfterClientClose() {
	t := s.T()
	client := s.newClient()

	owner := uuid.New()
	id := s.insertMedia(owner, "stored")

	stream, err := client.DownloadStream(s.ctx, owner, id, mediaservice.VariantOriginal)
	require.NoError(t, err)

	require.NoError(t, client.Close())
	require.NoError(t, stream.Close())
}

// TestDownloadStream_HidesStorageMethods - обёртка не выпускает наружу
// методы объекта хранилища.
//
// Проверка контракта, а не реализации: у объекта MinIO есть Seek и ReadAt,
// и если кто-то решит их вернуть, пусть сначала объяснит зачем. Проверка
// динамическая - статическое присваивание интерфейсу здесь ничего бы
// не поймало, тип переменной и так io.ReadCloser по сигнатуре метода.
func (s *ClientSuite) TestDownloadStream_HidesStorageMethods() {
	t := s.T()
	client := s.newClient()
	defer func() { _ = client.Close() }()

	owner := uuid.New()
	id := s.insertMedia(owner, "stored")

	stream, err := client.DownloadStream(s.ctx, owner, id, mediaservice.VariantOriginal)
	require.NoError(t, err)
	defer func() { _ = stream.Close() }()

	_, isSeeker := stream.(io.Seeker)
	require.False(t, isSeeker, "поток не должен обещать Seek: контракт хранилища этого не гарантирует")
	_, isReaderAt := stream.(io.ReaderAt)
	require.False(t, isReaderAt, "поток не должен обещать ReadAt: чтение произвольного смещения хранилищем не гарантировано")
}
