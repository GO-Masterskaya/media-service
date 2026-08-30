package media

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestParamsFingerprint_stable(t *testing.T) {
	t.Parallel()
	expires := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	a := ParamsFingerprint("image/png", true, false, &expires)
	b := ParamsFingerprint("image/png", true, false, &expires)
	if a != b || a == "" {
		t.Fatalf("unstable fingerprint: %q vs %q", a, b)
	}
}

func TestParamsFingerprint_differsOnFlags(t *testing.T) {
	t.Parallel()
	a := ParamsFingerprint("image/png", true, false, nil)
	b := ParamsFingerprint("image/png", false, false, nil)
	c := ParamsFingerprint("image/jpeg", true, false, nil)
	if a == b || a == c {
		t.Fatalf("expected different fingerprints: a=%s b=%s c=%s", a, b, c)
	}
}

func TestBodyFingerprint(t *testing.T) {
	t.Parallel()
	fp, n, err := BodyFingerprint(strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("n=%d want 5", n)
	}
	fp2, _, err := BodyFingerprint(bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatal(err)
	}
	if fp != fp2 {
		t.Fatalf("got %s want %s", fp2, fp)
	}
	fp3, _, err := BodyFingerprint(strings.NewReader("hallo"))
	if err != nil {
		t.Fatal(err)
	}
	if fp == fp3 {
		t.Fatal("different bodies must differ")
	}
}
