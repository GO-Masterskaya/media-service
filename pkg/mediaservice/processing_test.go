package mediaservice_test

import (
	"bytes"
	"image/color"
	"os/exec"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"mediaservice/pkg/mediaservice"
)

// requireFFmpeg пропускает тест, если нет ffmpeg или ffprobe.
//
// Движку нужны оба: ffprobe определяет тип файла, ffmpeg делает миниатюру.
// В CI они ставятся, локально могут отсутствовать.
func (s *ClientSuite) requireFFmpeg() {
	s.T().Helper()
	for _, bin := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(bin); err != nil {
			s.T().Skipf("%s not installed, skipping processing test", bin)
		}
	}
}

// TestProcessing_ProducesThumbnail - движок доводит объект до Ready
// и создаёт производную.
//
// Единственный тест, который проверяет обработку от начала до конца:
// загрузка с флагом, работа воркера, появление строки в media_derivative
// и заполнение Derivatives на выходе GetMedia.
//
// Про устойчивость. Ожидание идёт опросом состояния, а не сном
// на фиксированное время: Eventually завершится сразу, как только объект
// станет готов, и не развалится, если в CI под нагрузкой ffmpeg
// на холодном кеше задумается. Потолок в минуту выглядит избыточным
// намеренно - тест, падающий раз в двадцать прогонов, хуже отсутствующего,
// потому что его начинают перезапускать не глядя.
//
// PollInterval снижен до 50мс: умолчание в секунду разумно в бою
// и расточительно здесь. Заодно это проверяет, что WithProcessingConfig
// включает движок сам, без отдельной WithProcessing.
func (s *ClientSuite) TestProcessing_ProducesThumbnail() {
	s.requireFFmpeg()

	t := s.T()
	client := s.newClient(mediaservice.WithProcessingConfig(mediaservice.ProcessingConfig{
		WorkerConcurrency: 1,
		PollInterval:      50 * time.Millisecond,
		TempDir:           t.TempDir(),
	}))
	defer func() { _ = client.Close() }()

	owner := uuid.New()
	// 64 пикселя, а не один: обработчику нужно что-то уменьшать,
	// и на вырожденной картинке ffmpeg ведёт себя непредсказуемо.
	body := testPNG(t, 64, color.RGBA{R: 90, G: 160, B: 220, A: 255})

	res, err := client.Upload(s.ctx, mediaservice.UploadParams{
		OwnerID:        owner,
		Filename:       "picture.png",
		MIMEType:       "image/png",
		ExpectedSize:   uint64(len(body)),
		IdempotencyKey: uuid.NewString(),
		Processing:     mediaservice.ProcessingOptions{MakeThumbnail: true},
	}, bytes.NewReader(body))
	require.NoError(t, err)
	require.Equal(t, mediaservice.StatusProcessing, res.Status,
		"с поднятым движком флаги не гасятся и задача ставится в очередь")

	var final *mediaservice.Media
	require.Eventually(t, func() bool {
		m, err := client.GetMedia(s.ctx, owner, res.ID)
		if err != nil {
			return false
		}
		final = m
		return m.Status == mediaservice.StatusReady || m.Status == mediaservice.StatusFailed
	}, time.Minute, 200*time.Millisecond, "объект не вышел из статуса Processing")

	require.Equal(t, mediaservice.StatusReady, final.Status,
		"обработка завершилась ошибкой: %s", final.Error)
	require.NotEmpty(t, final.Derivatives)

	var found bool
	for _, d := range final.Derivatives {
		if d.Variant == mediaservice.VariantThumb {
			found = true
			require.NotZero(t, d.SizeBytes)
			require.NotEmpty(t, d.MIMEType)
		}
	}
	require.True(t, found, "среди производных нет миниатюры")
}

// TestProcessing_ThumbIsDownloadable - после обработки производная
// реально отдаётся, а не только числится в базе.
//
// Отдельный тест, потому что проверяет другую вещь: строка
// в media_derivative могла появиться при неудачной выгрузке в хранилище,
// и GetDownloadURL это обнаружит, а GetMedia нет.
func (s *ClientSuite) TestProcessing_ThumbIsDownloadable() {
	s.requireFFmpeg()

	t := s.T()
	client := s.newClient(mediaservice.WithProcessingConfig(mediaservice.ProcessingConfig{
		WorkerConcurrency: 1,
		PollInterval:      50 * time.Millisecond,
		TempDir:           t.TempDir(),
	}))
	defer func() { _ = client.Close() }()

	owner := uuid.New()
	body := testPNG(t, 64, color.RGBA{R: 20, G: 200, B: 90, A: 255})

	res, err := client.Upload(s.ctx, mediaservice.UploadParams{
		OwnerID:        owner,
		MIMEType:       "image/png",
		ExpectedSize:   uint64(len(body)),
		IdempotencyKey: uuid.NewString(),
		Processing:     mediaservice.ProcessingOptions{MakeThumbnail: true},
	}, bytes.NewReader(body))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		m, err := client.GetMedia(s.ctx, owner, res.ID)
		return err == nil && m.Status == mediaservice.StatusReady
	}, time.Minute, 200*time.Millisecond)

	link, err := client.GetDownloadURL(s.ctx, owner, res.ID, mediaservice.VariantThumb)
	require.NoError(t, err)
	require.NotEmpty(t, link.URL)
	require.True(t, link.ExpiresAt.After(time.Now()))
}

// TestProcessing_RequiresFFmpeg - отсутствие бинарника даёт понятную
// ошибку на конструкторе.
//
// Без этой проверки движок поднялся бы, разобрал очередь и пометил каждую
// задачу неудачной - но не сразу, а после трёх попыток с нарастающей
// отсрочкой. Причина осталась бы в логах воркера, куда вызывающий
// не смотрит.
//
// PATH подменяется на пустой: exec.LookPath перестаёт находить что-либо,
// и это единственный способ воспроизвести окружение без ffmpeg,
// не удаляя его с машины.
func (s *ClientSuite) TestProcessing_RequiresFFmpeg() {
	t := s.T()
	t.Setenv("PATH", "")

	pool := s.newExternalPool()
	t.Cleanup(pool.Close)

	_, err := mediaservice.NewWithDeps(s.ctx, mediaservice.Deps{
		Pool:   pool,
		MinIO:  s.newExternalMinIO(),
		Bucket: testBucket,
	}, mediaservice.WithProcessing())

	require.ErrorIs(t, err, mediaservice.ErrInternal)
	require.Contains(t, err.Error(), "ffmpeg")
}

// TestClient_TwoInstancesInOneProcess - два клиента в одном процессе
// создаются без паники.
//
// Проверка изолированного реестра Prometheus. Внутренние счётчики
// регистрируются через MustRegister, а он паникует на повторной
// регистрации метрики с тем же именем. Умолчание prometheus.NewRegistry()
// это предотвращает, но выглядит перестраховкой, пока нет теста:
// без него первый же человек, решивший "зачем тут свой реестр",
// поменяет его на nil и уронит чужое приложение.
//
// Паника не ловится через require: если она случится, тест упадёт сам,
// и этого достаточно.
func (s *ClientSuite) TestClient_TwoInstancesInOneProcess() {
	t := s.T()

	first := s.newClient()
	defer func() { _ = first.Close() }()

	second := s.newClient()
	defer func() { _ = second.Close() }()

	owner := uuid.New()
	id := s.insertMedia(owner, "stored")

	for _, client := range []*mediaservice.Client{first, second} {
		got, err := client.GetMedia(s.ctx, owner, id)
		require.NoError(t, err)
		require.Equal(t, id, got.ID)
	}
}
