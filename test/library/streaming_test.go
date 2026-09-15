package library_test

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"runtime"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/GO-Masterskaya/media-service/pkg/mediaservice"
)

// Размеры полезной нагрузки для сравнения.
//
// Оба под 16 МиБ намеренно. minio-go переключается на multipart, когда объект
// больше размера части, а по умолчанию это ровно 16 МиБ (minPartSize). На
// multipart клиент хранилища заводит буферы под части, и их объём зависит от
// числа частей - то есть от размера файла. Это поведение библиотеки хранилища,
// а не нашей, но в измерение оно попало бы и сделало результат бессмысленным.
//
// Под порогом обе загрузки идут одним PUT, потоком, и разница в пике остаётся
// разницей в поведении именно mediaservice.
const (
	smallPayload = 256 << 10 // 256 КиБ
	largePayload = 15 << 20  // 15 МиБ
)

// maxPeakGrowth - сколько пику разрешено вырасти при росте файла в 60 раз.
//
// Порог грубый и таким задуман. Задача не измерить потребление, а поймать
// пропорциональный рост: если бы файл поднимался в память целиком, разница
// составила бы около 14,75 МиБ - весь разрыв между размерами. Порог вдвое
// меньше разрыва, так что настоящая буферизация не пролезет, а шум
// сборщика мусора не уронит тест.
//
// Ожидаемая величина при потоковой работе - близкая к нулю: библиотека читает
// загрузку буфером в 1 МиБ, тело пишет во временный файл, а на чтении io.Copy
// работает буфером в 32 КиБ. Ни одно из этих чисел от размера файла не зависит.
const maxPeakGrowth = 6 << 20 // 6 МиБ

// TestStreamingPeakDoesNotScaleWithFileSize - критерий приёмки про потоковость.
//
// Прямое измерение абсолютного потребления бессмысленно: оно зависит от того,
// что процесс делал до теста, и от момента сборки мусора. Поэтому меряются два
// прогона одного и того же цикла, на маленьком файле и на большом, и
// сравниваются приросты пика.
//
// Пик берётся выборкой в фоне, а не разностью до и после. Разность до и после
// показала бы удержанную память, а буферизация всего файла - память
// transient: к моменту второго замера сборщик её уже освободит, и тест ничего
// не заметит.
func (s *LibrarySuite) TestStreamingPeakDoesNotScaleWithFileSize() {
	t := s.T()
	s.requireFFprobe()

	smallPath := tempWAV(t, "small.wav", smallPayload)
	largePath := tempWAV(t, "large.wav", largePayload)

	// Прогрев. Первый цикл тянет за собой ленивую инициализацию пула,
	// HTTP-клиента хранилища и буферов; без него он выглядел бы как всплеск
	// и приписался бы маленькому файлу.
	s.uploadDownloadCycle(t, smallPath)

	smallPeak := measurePeakHeap(func() { s.uploadDownloadCycle(t, smallPath) })
	largePeak := measurePeakHeap(func() { s.uploadDownloadCycle(t, largePath) })

	var growth uint64
	if largePeak > smallPeak {
		growth = largePeak - smallPeak
	}

	t.Logf("пик кучи: %d КиБ на %d КиБ файле, %d КиБ на %d КиБ файле, прирост %d КиБ",
		smallPeak>>10, smallPayload>>10, largePeak>>10, largePayload>>10, growth>>10)

	require.Less(t, growth, uint64(maxPeakGrowth),
		"пик памяти вырос на %d КиБ при росте файла на %d КиБ - похоже, "+
			"файл поднимается в память целиком вместо потоковой обработки",
		growth>>10, (largePayload-smallPayload)>>10)
}

// uploadDownloadCycle прогоняет файл через библиотеку и обратно, нигде не
// собирая его в памяти: загрузка читается с диска, выгрузка уходит в хеш.
//
// Хеш здесь не только ради проверки: он заставляет реально вычитать поток.
// io.Copy в io.Discard теоретически мог бы быть срезан оптимизацией через
// ReadFrom, и тогда тест мерил бы не то.
func (s *LibrarySuite) uploadDownloadCycle(t require.TestingT, path string) {
	owner := uuid.New()

	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	require.NoError(t, err)

	res, err := s.client.Upload(s.ctx, mediaservice.UploadParams{
		OwnerID:        owner,
		Filename:       "payload.wav",
		MIMEType:       "audio/wav",
		ExpectedSize:   uint64(info.Size()),
		IdempotencyKey: uuid.NewString(),
	}, f)
	require.NoError(t, err)

	rc, err := s.client.DownloadStream(s.ctx, owner, res.ID, mediaservice.VariantOriginal)
	require.NoError(t, err)
	defer func() { _ = rc.Close() }()

	h := sha256.New()
	n, err := io.Copy(h, rc)
	require.NoError(t, err)
	require.Equal(t, info.Size(), n, "скачано не столько байт, сколько загружено")
	require.Equal(t, sha256File(t, path), hex.EncodeToString(h.Sum(nil)),
		"содержимое изменилось при передаче")
}

// measurePeakHeap возвращает, насколько куча поднималась над исходным уровнем
// за время работы f.
//
// Выборка, а не непрерывное наблюдение: дешёвого способа узнать пик в Go нет,
// runtime.ReadMemStats останавливает мир. Шаг в 5 мс - компромисс: цикл
// занимает секунды, и удержание файла в памяти растянулось бы на сотни
// выборок, пропустить его невозможно. Короткий всплеск на одну аллокацию
// выборка действительно может не заметить, но такой всплеск и не был бы
// признаком буферизации всего файла.
func measurePeakHeap(f func()) uint64 {
	runtime.GC()

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	base := ms.HeapAlloc

	stop := make(chan struct{})
	result := make(chan uint64, 1)

	go func() {
		var peak uint64
		var sample runtime.MemStats
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()

		for {
			select {
			case <-stop:
				result <- peak
				return
			case <-tick.C:
				runtime.ReadMemStats(&sample)
				if sample.HeapAlloc > peak {
					peak = sample.HeapAlloc
				}
			}
		}
	}()

	f()
	close(stop)
	peak := <-result

	if peak < base {
		return 0
	}
	return peak - base
}
