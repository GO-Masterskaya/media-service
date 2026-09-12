package mediaservice_test

import (
	"reflect"
	"testing"

	"mediaservice/pkg/mediaservice"
	mediav1 "mediaservice/proto/media/v1"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestGetMedia_MapsAllFields - сверка публичной модели с содержимым записи.
//
// Проверяются все поля разом, а не только имя файла. Причина в природе
// ошибки, от которой страхует тест: забытое поле в toPublicMedia не даёт
// ни ошибки компиляции, ни падения - вызывающий просто получает нулевое
// значение и считает, что данных нет. Поймать это можно только сверкой
// полного состава.
func (s *ClientSuite) TestGetMedia_MapsAllFields() {
	t := s.T()
	client := s.newClient()
	defer func() { _ = client.Close() }()

	owner := uuid.New()
	// Пробел и кириллица намеренно: имя возвращается как пришло,
	// нормализации на пути наружу нет.
	const filename = "отчёт за квартал.pdf"
	id := s.insertMediaWithFilename(owner, "stored", filename)

	got, err := client.GetMedia(s.ctx, owner, id)
	require.NoError(t, err)

	require.Equal(t, id, got.ID)
	require.Equal(t, owner, got.OwnerID)
	require.Equal(t, mediaservice.KindImage, got.Kind)
	require.Equal(t, "image/jpeg", got.MIMEType)
	require.Equal(t, uint64(1024), got.SizeBytes)
	require.Equal(t, mediaservice.StatusStored, got.Status)
	require.Equal(t, filename, got.Filename)
	require.Empty(t, got.Metadata, "вставка не задавала metadata, база подставила {}")
	require.Empty(t, got.Derivatives, "GetMedia не подтягивает производные")
	require.Empty(t, got.Error)
	require.False(t, got.CreatedAt.IsZero())
}

// TestGetMedia_EmptyFilename - пустое имя это допустимое значение.
//
// В UploadInit поле filename необязательное, а колонка orig_filename
// объявлена NOT NULL, поэтому в базе окажется пустая строка. Наружу она
// должна выйти пустой строкой, а не ошибкой разбора записи.
func (s *ClientSuite) TestGetMedia_EmptyFilename() {
	t := s.T()
	client := s.newClient()
	defer func() { _ = client.Close() }()

	owner := uuid.New()
	id := s.insertMediaWithFilename(owner, "stored", "")

	got, err := client.GetMedia(s.ctx, owner, id)
	require.NoError(t, err)
	require.Empty(t, got.Filename)
}

// protoToPublicMediaField связывает поля message Media с полями публичной
// структуры Media. Таблица ведётся руками: имена по обе стороны выбирались
// независимо (mime против MIMEType), вывести одно из другого нельзя.
var protoToPublicMediaField = map[string]string{
	"id":          "ID",
	"owner_id":    "OwnerID",
	"kind":        "Kind",
	"mime":        "MIMEType",
	"size_bytes":  "SizeBytes",
	"status":      "Status",
	"metadata":    "Metadata",
	"derivatives": "Derivatives",
	"error":       "Error",
	"created_at":  "CreatedAt",
	"filename":    "Filename",
}

// TestMediaShapeMatchesProto - состав полей библиотеки совпадает с message Media.
//
// Инвариант зафиксирован в ревью: библиотека и gRPC это два публичных API
// одного сервиса, расходиться по составу полей они не должны. Само поле
// filename появилось именно потому, что расхождение никто не замечал:
// имя записывалось в базу, но прочитать его было нельзя ни одним из двух
// способов.
//
// Тест сверяет обе стороны. Поле, добавленное в proto и забытое в pkg,
// компилируется и проходит все остальные тесты - без этой проверки
// расхождение снова станет незаметным.
//
// Контейнеры не нужны, поэтому тест идёт и в режиме -short.
func TestMediaShapeMatchesProto(t *testing.T) {
	protoFields := (&mediav1.Media{}).ProtoReflect().Descriptor().Fields()

	goFields := make(map[string]struct{})
	rt := reflect.TypeOf(mediaservice.Media{})
	for i := 0; i < rt.NumField(); i++ {
		goFields[rt.Field(i).Name] = struct{}{}
	}

	matched := make(map[string]struct{}, protoFields.Len())
	for i := 0; i < protoFields.Len(); i++ {
		name := string(protoFields.Get(i).Name())

		goName, ok := protoToPublicMediaField[name]
		require.Truef(t, ok,
			"поле %q есть в message Media, но не описано в protoToPublicMediaField", name)
		require.Containsf(t, goFields, goName,
			"полю %q из proto сопоставлено %s, которого нет в mediaservice.Media", name, goName)

		matched[goName] = struct{}{}
	}

	for goName := range goFields {
		require.Containsf(t, matched, goName,
			"поле %s есть в mediaservice.Media, но ему не соответствует ни одно поле message Media", goName)
	}
}
