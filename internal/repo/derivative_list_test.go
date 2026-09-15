package repo

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestListByMediaIDs(t *testing.T) {
	pool := setupPostgres(t)
	mediaRepo := NewPgMediaRepo(pool)
	derivRepo := NewPgDerivativeRepo(pool)
	ctx := context.Background()

	resetDB(t, pool)

	owner := uuid.New()
	mediaOne := sampleMedia(owner, "deriv-list-1", "body-1", "params-1")
	mediaTwo := sampleMedia(owner, "deriv-list-2", "body-2", "params-2")

	if _, err := mediaRepo.InsertWithJobs(ctx, mediaOne, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := mediaRepo.InsertWithJobs(ctx, mediaTwo, nil); err != nil {
		t.Fatal(err)
	}

	first := Derivative{
		MediaID:    mediaOne.ID,
		Variant:    "thumbnail",
		Mime:       "image/jpeg",
		SizeBytes:  100,
		StorageKey: "thumb-1",
	}
	second := Derivative{
		MediaID:    mediaOne.ID,
		Variant:    "preview",
		Mime:       "image/jpeg",
		SizeBytes:  200,
		StorageKey: "preview-1",
	}
	third := Derivative{
		MediaID:    mediaTwo.ID,
		Variant:    "thumbnail",
		Mime:       "image/jpeg",
		SizeBytes:  300,
		StorageKey: "thumb-2",
	}

	for _, d := range []Derivative{first, second, third} {
		if _, err := derivRepo.Insert(ctx, d); err != nil {
			t.Fatal(err)
		}
	}

	got, err := derivRepo.ListByMediaIDs(ctx, []uuid.UUID{mediaOne.ID, mediaTwo.ID})
	if err != nil {
		t.Fatal(err)
	}

	if len(got[mediaOne.ID]) != 2 {
		t.Fatalf("mediaOne derivatives=%d, want 2", len(got[mediaOne.ID]))
	}
	if len(got[mediaTwo.ID]) != 1 {
		t.Fatalf("mediaTwo derivatives=%d, want 1", len(got[mediaTwo.ID]))
	}

	if got[mediaOne.ID][0].Variant != "thumbnail" {
		t.Fatalf("first variant=%q, want thumbnail", got[mediaOne.ID][0].Variant)
	}
	if got[mediaOne.ID][1].Variant != "preview" {
		t.Fatalf("second variant=%q, want preview", got[mediaOne.ID][1].Variant)
	}
}

func TestListByMediaIDs_EmptyIDs(t *testing.T) {
	pool := setupPostgres(t)
	derivRepo := NewPgDerivativeRepo(pool)

	got, err := derivRepo.ListByMediaIDs(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	if got == nil {
		t.Fatal("got nil map, want empty map")
	}
	if len(got) != 0 {
		t.Fatalf("got %d media IDs, want 0", len(got))
	}
}
