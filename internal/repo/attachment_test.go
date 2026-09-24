package repo

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestHasAttachment(t *testing.T) {
	pool := setupPostgres(t)
	mediaRepo := NewPgMediaRepo(pool)
	ctx := context.Background()

	resetDB(t, pool)
	owner := uuid.New()
	attacher := uuid.New()
	stranger := uuid.New()

	created, err := mediaRepo.InsertWithJobs(ctx, sampleMedia(owner, "att-key", "body-att", "params-att"), nil)
	if err != nil {
		t.Fatal(err)
	}

	ok, err := mediaRepo.HasAttachment(ctx, created.ID, owner)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("owner attachment missing after InsertWithJobs")
	}

	ok, err = mediaRepo.HasAttachment(ctx, created.ID, stranger)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("stranger unexpectedly has attachment")
	}

	if err := mediaRepo.CreateAttachment(ctx, created.ID, attacher); err != nil {
		t.Fatal(err)
	}
	ok, err = mediaRepo.HasAttachment(ctx, created.ID, attacher)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("attacher missing after CreateAttachment")
	}

	if _, err := mediaRepo.DeleteAttachment(ctx, created.ID, attacher); err != nil {
		t.Fatal(err)
	}
	ok, err = mediaRepo.HasAttachment(ctx, created.ID, attacher)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("attacher still attached after DeleteAttachment")
	}

	missing := uuid.New()
	ok, err = mediaRepo.HasAttachment(ctx, missing, owner)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("HasAttachment true for unknown media_id")
	}
}
