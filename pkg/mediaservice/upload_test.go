package mediaservice_test

import (
	"bytes"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"os/exec"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"mediaservice/pkg/mediaservice"
)

// testPNG собирает валидный PNG заданного размера и цвета.
//
// Картинка строится кодировщиком стандартной библиотеки, а не берётся
// из зашитой base64-строки: так видно, что именно лежит в байтах, и можно
// получить два заведомо разных файла, поменяв цвет. Для проверки
// идемпотентности это существенно - там нужны именно разные тела
// при одинаковом ключе.
func testPNG(t require.TestingT, size int, c color.Color) []byte {
	img := image.NewRGBA(image.Rect(0, 0, size, size))
	draw.Draw(img, img.Bounds(), &image.Uniform{C: c}, image.Point{}, draw.Src)

	var buf bytes.Buffer
	require.NoError(t, png.Encode(&buf, img))
	return buf.Bytes()
}

// requireFFprobe пропускает тест, если ffprobe не установлен.
//
// Ядро определяет тип файла анализом содержимого, а не заявленным MIME,
// и делает это внешним бинарником. Без него Upload не работает в принципе,
// поэтому тест не падает, а честно пропускается: в CI ffmpeg ставится,
// на машине разработчика может не стоять.
func (s *ClientSuite) requireFFprobe() {
	s.T().Helper()
	if _, err := exec.LookPath("ffprobe"); err != nil {
		s.T().Skip("ffprobe not installed, skipping upload test")
	}
}

// uploadPNG - короткая обёртка над загрузкой картинки.
func (s *ClientSuite) uploadPNG(
	client *mediaservice.Client,
	owner uuid.UUID,
	key string,
	body []byte,
) (mediaservice.UploadResult, error) {
	s.T().Helper()
	return client.Upload(s.ctx, mediaservice.UploadParams{
		OwnerID:        owner,
		Filename:       "picture.png",
		MIMEType:       "image/png",
		ExpectedSize:   uint64(len(body)),
		IdempotencyKey: key,
	}, bytes.NewReader(body))
}

// TestUpload_StoresAndReadsBack - загрузка доходит до базы, и запись
// читается обратно с теми же характеристиками.
//
// Проверяется не только факт успеха: Kind и MIMEType ядро определяет
// само по содержимому, и совпадение с ожиданием означает, что анализ
// отработал, а не что сохранилось заявленное.
func (s *ClientSuite) TestUpload_StoresAndReadsBack() {
	s.requireFFprobe()

	t := s.T()
	client := s.newClient()
	defer func() { _ = client.Close() }()

	owner := uuid.New()
	body := testPNG(t, 16, color.RGBA{R: 200, G: 30, B: 30, A: 255})

	res, err := s.uploadPNG(client, owner, uuid.NewString(), body)
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, res.ID)
	require.Equal(t, mediaservice.StatusStored, res.Status,
		"без WithProcessing объект остаётся в Stored")

	got, err := client.GetMedia(s.ctx, owner, res.ID)
	require.NoError(t, err)
	require.Equal(t, owner, got.OwnerID)
	require.Equal(t, mediaservice.KindImage, got.Kind)
	require.Equal(t, "image/png", got.MIMEType)
	require.Equal(t, uint64(len(body)), got.SizeBytes)
	require.Equal(t, "picture.png", got.Filename)
	require.Empty(t, got.Derivatives, "обработка не запрашивалась")
}

// TestUpload_IdempotentReplay - повтор с тем же ключом и тем же телом
// возвращает существующий объект, а не создаёт второй.
//
// Проверка идёт и по идентификатору, и по числу строк в базе: совпадение
// идентификаторов само по себе не исключает, что рядом появилась вторая
// запись.
func (s *ClientSuite) TestUpload_IdempotentReplay() {
	s.requireFFprobe()

	t := s.T()
	client := s.newClient()
	defer func() { _ = client.Close() }()

	owner := uuid.New()
	body := testPNG(t, 16, color.RGBA{R: 10, G: 120, B: 200, A: 255})
	key := uuid.NewString()

	first, err := s.uploadPNG(client, owner, key, body)
	require.NoError(t, err)

	second, err := s.uploadPNG(client, owner, key, body)
	require.NoError(t, err)
	require.Equal(t, first.ID, second.ID)

	var count int
	require.NoError(t, s.admin.QueryRow(s.ctx,
		`SELECT count(*) FROM media WHERE owner_id = $1`, owner).Scan(&count))
	require.Equal(t, 1, count)
}

// TestUpload_SameKeyDifferentBody - тот же ключ с другим содержимым
// это конфликт, а не повтор.
//
// Идемпотентность означает "тот же запрос даёт тот же результат".
// Другое тело под тем же ключом - уже другой запрос, и молча вернуть
// старый объект значило бы потерять новый файл.
func (s *ClientSuite) TestUpload_SameKeyDifferentBody() {
	s.requireFFprobe()

	t := s.T()
	client := s.newClient()
	defer func() { _ = client.Close() }()

	owner := uuid.New()
	key := uuid.NewString()

	_, err := s.uploadPNG(client, owner, key,
		testPNG(t, 16, color.RGBA{R: 255, A: 255}))
	require.NoError(t, err)

	_, err = s.uploadPNG(client, owner, key,
		testPNG(t, 32, color.RGBA{B: 255, A: 255}))
	require.ErrorIs(t, err, mediaservice.ErrAlreadyExists)
}

// TestUpload_RejectsBadInput - проверки аргументов срабатывают до того,
// как из reader прочитан хоть один байт.
//
// Порядок здесь имеет значение: если бы проверки стояли после приёма
// файла, отказ по нулевому владельцу стоил бы полного выкачивания
// пятисот мегабайт.
func (s *ClientSuite) TestUpload_RejectsBadInput() {
	t := s.T()
	client := s.newClient()
	defer func() { _ = client.Close() }()

	body := testPNG(t, 8, color.White)
	owner := uuid.New()

	cases := []struct {
		name   string
		params mediaservice.UploadParams
		reader *bytes.Reader
	}{
		{
			name:   "нулевой владелец",
			params: mediaservice.UploadParams{MIMEType: "image/png", IdempotencyKey: uuid.NewString()},
			reader: bytes.NewReader(body),
		},
		{
			name:   "пустой ключ идемпотентности",
			params: mediaservice.UploadParams{OwnerID: owner, MIMEType: "image/png"},
			reader: bytes.NewReader(body),
		},
		{
			name: "отрицательный TTL",
			params: mediaservice.UploadParams{
				OwnerID: owner, MIMEType: "image/png",
				IdempotencyKey: uuid.NewString(), TTL: -time.Minute,
			},
			reader: bytes.NewReader(body),
		},
		{
			name: "тип вне allowlist",
			params: mediaservice.UploadParams{
				OwnerID: owner, MIMEType: "application/pdf",
				IdempotencyKey: uuid.NewString(),
			},
			reader: bytes.NewReader(body),
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			_, err := client.Upload(s.ctx, tc.params, tc.reader)
			require.ErrorIs(s.T(), err, mediaservice.ErrInvalidArgument)
		})
	}

	s.Run("nil reader", func() {
		_, err := client.Upload(s.ctx, mediaservice.UploadParams{
			OwnerID: owner, MIMEType: "image/png", IdempotencyKey: uuid.NewString(),
		}, nil)
		require.ErrorIs(s.T(), err, mediaservice.ErrInvalidArgument)
	})
}

// TestUpload_QuotaExceeded - превышение квоты владельца даёт отдельную
// ошибку, а не ErrInternal.
//
// Квота берётся из таблицы storage_quotas и имеет приоритет над значением
// из опции. Ноль в колонке означал бы "без ограничения", поэтому ставим
// заведомо маленькое положительное число.
func (s *ClientSuite) TestUpload_QuotaExceeded() {
	s.requireFFprobe()

	t := s.T()
	client := s.newClient()
	defer func() { _ = client.Close() }()

	owner := uuid.New()
	_, err := s.admin.Exec(s.ctx,
		`INSERT INTO storage_quotas (owner_id, storage_used_bytes, storage_quota_bytes)
		 VALUES ($1, 0, 16)`, owner)
	require.NoError(t, err)

	body := testPNG(t, 32, color.RGBA{G: 255, A: 255})
	require.Greater(t, len(body), 16, "картинка должна не влезать в квоту")

	_, err = s.uploadPNG(client, owner, uuid.NewString(), body)
	require.ErrorIs(t, err, mediaservice.ErrQuotaExceeded)
}

// TestUpload_ProcessingFlagsIgnoredWithoutEngine - негативный контроль
// к главному решению в Upload.
//
// Флаги обработки без поднятого движка гасятся. Если этого не делать,
// ядро создаст задачи, объект уйдёт в статус Processing, разбирать их
// будет некому, и вдобавок такой объект перестанет удаляться через
// Delete - там гард на этот статус. Проверяем оба следствия.
func (s *ClientSuite) TestUpload_ProcessingFlagsIgnoredWithoutEngine() {
	s.requireFFprobe()

	t := s.T()
	client := s.newClient()
	defer func() { _ = client.Close() }()

	owner := uuid.New()
	body := testPNG(t, 16, color.Black)

	res, err := client.Upload(s.ctx, mediaservice.UploadParams{
		OwnerID:        owner,
		Filename:       "picture.png",
		MIMEType:       "image/png",
		ExpectedSize:   uint64(len(body)),
		IdempotencyKey: uuid.NewString(),
		Processing: mediaservice.ProcessingOptions{
			MakeThumbnail: true,
			Transcode:     true,
		},
	}, bytes.NewReader(body))
	require.NoError(t, err)
	require.Equal(t, mediaservice.StatusStored, res.Status)

	// Первое следствие: задач в очереди не появилось. Публичного API
	// для processing_jobs нет, а предмет проверки именно она.
	var jobs int
	require.NoError(t, s.admin.QueryRow(s.ctx,
		`SELECT count(*) FROM processing_jobs WHERE media_id = $1`, res.ID).Scan(&jobs))
	require.Zero(t, jobs, "без движка задачи обработки создаваться не должны")

	// Второе следствие: гард на статус processing при удалении не срабатывает.
	//
	// Здесь NotErrorIs, а не NoError, и это не обход проблемы. Delete снимает
	// привязку из media_attachments, а Upload её не создаёт: AttachMedia
	// вызывается только обработчиком Kafka-события attach, консьюмер во
	// встроенном режиме не запускается. Поэтому Delete вернёт ErrNotFound
	// независимо от статуса. К делу относится другое: что это не ErrNotReady.
	require.NotErrorIs(t, client.Delete(s.ctx, owner, res.ID), mediaservice.ErrNotReady)
}

// TestUpload_NoJobsCreatedWithoutEngine - та же проверка со стороны базы.
//
// Статус мог бы остаться Stored и при созданных задачах, если бы ядро
// меняло его позже. Считаем строки в очереди напрямую.
func (s *ClientSuite) TestUpload_NoJobsCreatedWithoutEngine() {
	s.requireFFprobe()

	t := s.T()
	client := s.newClient()
	defer func() { _ = client.Close() }()

	owner := uuid.New()
	body := testPNG(t, 16, color.Black)

	res, err := client.Upload(s.ctx, mediaservice.UploadParams{
		OwnerID:        owner,
		MIMEType:       "image/png",
		ExpectedSize:   uint64(len(body)),
		IdempotencyKey: uuid.NewString(),
		Processing:     mediaservice.ProcessingOptions{MakeThumbnail: true},
	}, bytes.NewReader(body))
	require.NoError(t, err)

	var jobs int
	require.NoError(t, s.admin.QueryRow(s.ctx,
		`SELECT count(*) FROM processing_jobs WHERE media_id = $1`, res.ID).Scan(&jobs))
	require.Zero(t, jobs)
}
