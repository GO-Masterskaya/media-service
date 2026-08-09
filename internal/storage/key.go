package storage

import (
	"fmt"
	"path"
	"strings"

	"github.com/google/uuid"
)

// Layout: {owner_id}/{media_id}/{variant}.{ext}
// Сырой filename НИКОГДА не попадает в ключ — только для вывода безопасного расширения.

type Variant string

const (
	VariantOriginal Variant = "original"
	VariantThumb    Variant = "thumb"
	VariantPreview  Variant = "preview"
	VariantR720     Variant = "r_720"
	VariantR360     Variant = "r_360"
)

// BuildKey строит storage key по layout SPEC.
// filename используется ТОЛЬКО как fallback для расширения; само имя не утекает в ключ.
func BuildKey(ownerID, mediaID uuid.UUID, variant Variant, mimeType, filename string) (string, error) {
	if ownerID == uuid.Nil || mediaID == uuid.Nil {
		return "", fmt.Errorf("owner_id and media_id must be valid UUIDs")
	}

	ext := extFromMime(mimeType)
	if ext == "" {
		ext = extFromFilename(filename)
	}
	if ext == "" {
		ext = "bin"
	}
	ext = sanitizeExt(ext)

	key := path.Join(ownerID.String(), mediaID.String(), string(variant)+"."+ext)
	if strings.Contains(key, "..") {
		return "", fmt.Errorf("invalid key construction")
	}
	return key, nil
}

func extFromMime(mime string) string {
	switch {
	case strings.HasPrefix(mime, "image/jpeg"):
		return "jpg"
	case strings.HasPrefix(mime, "image/png"):
		return "png"
	case strings.HasPrefix(mime, "image/webp"):
		return "webp"
	case strings.HasPrefix(mime, "image/gif"):
		return "gif"
	case strings.HasPrefix(mime, "video/mp4"):
		return "mp4"
	case strings.HasPrefix(mime, "video/"):
		return "mp4"
	case strings.HasPrefix(mime, "audio/mpeg"):
		return "mp3"
	case strings.HasPrefix(mime, "audio/"):
		return "mp3"
	default:
		return ""
	}
}

func extFromFilename(filename string) string {
	if filename == "" {
		return ""
	}
	ext := path.Ext(filename)
	if ext == "" {
		return ""
	}
	return strings.TrimPrefix(ext, ".")
}

func sanitizeExt(ext string) string {
	ext = strings.ToLower(ext)
	var b strings.Builder
	for _, r := range ext {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "bin"
	}
	return b.String()
}
