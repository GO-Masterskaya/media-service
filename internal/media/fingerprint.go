package media

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strconv"
	"time"
)

// BodyFingerprint считает sha256 hex содержимого reader'а.
// Возвращает также число прочитанных байт.
func BodyFingerprint(r io.Reader) (hexDigest string, n int64, err error) {
	h := sha256.New()
	n, err = io.Copy(h, r)
	if err != nil {
		return "", n, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// ParamsFingerprint — стабильный хэш существенных init-параметров upload
// (mime, processing flags, expires_at). Без filename и expected_size.
func ParamsFingerprint(mime string, makeThumbnail, transcode bool, expiresAt *time.Time) string {
	expires := ""
	if expiresAt != nil {
		expires = expiresAt.UTC().Format(time.RFC3339Nano)
	}
	canonical := mime + "\n" +
		strconv.FormatBool(makeThumbnail) + "\n" +
		strconv.FormatBool(transcode) + "\n" +
		expires
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}
