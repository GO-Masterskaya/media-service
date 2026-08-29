package repo

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestInsertWithJobs(t *testing.T) {
	pool := setupPostgres(t)
	mediaRepo := NewPgMediaRepo(pool)
	ctx := context.Background()

	t.Run("stored without jobs", func(t *testing.T) {
		resetDB(t, pool)
		m := sampleMedia(uuid.New(), "key-1", "body-a", "params-a")
		created, err := mediaRepo.InsertWithJobs(ctx, m, nil)
		if err != nil {
			t.Fatal(err)
		}
		if created.Status != MediaStatusStored {
			t.Fatalf("status %s, want stored", created.Status)
		}
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM processing_jobs WHERE media_id=$1`, created.ID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("jobs=%d want 0", n)
		}
	})

	t.Run("processing with jobs atomic", func(t *testing.T) {
		resetDB(t, pool)
		m := sampleMedia(uuid.New(), "key-2", "body-b", "params-b")
		created, err := mediaRepo.InsertWithJobs(ctx, m, []string{"thumbnail", "transcode"})
		if err != nil {
			t.Fatal(err)
		}
		if created.Status != MediaStatusProcessing {
			t.Fatalf("status %s, want processing", created.Status)
		}
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM processing_jobs WHERE media_id=$1`, created.ID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Fatalf("jobs=%d want 2", n)
		}
	})

	t.Run("get by owner idempotency", func(t *testing.T) {
		resetDB(t, pool)
		owner := uuid.New()
		m := sampleMedia(owner, "idem-1", "body-c", "params-c")
		created, err := mediaRepo.InsertWithJobs(ctx, m, nil)
		if err != nil {
			t.Fatal(err)
		}
		got, err := mediaRepo.GetByOwnerIdempotency(ctx, owner, "idem-1")
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != created.ID {
			t.Fatalf("id %v want %v", got.ID, created.ID)
		}
		if got.BodyFingerprint != "body-c" || got.ParamsFingerprint != "params-c" {
			t.Fatalf("fingerprints %q/%q", got.BodyFingerprint, got.ParamsFingerprint)
		}
	})

	t.Run("concurrent same key one wins", func(t *testing.T) {
		resetDB(t, pool)
		owner := uuid.New()
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		ids := make(chan uuid.UUID, 2)
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				m := sampleMedia(owner, "race-key", "body-same", "params-same")
				m.ID = uuid.New()
				created, err := mediaRepo.InsertWithJobs(ctx, m, []string{"thumbnail"})
				if err != nil {
					errs <- err
					return
				}
				ids <- created.ID
			}()
		}
		wg.Wait()
		close(errs)
		close(ids)

		var conflict, ok int
		for err := range errs {
			if errors.Is(err, ErrConcurrentConflict) {
				conflict++
				continue
			}
			t.Fatalf("unexpected: %v", err)
		}
		var seen []uuid.UUID
		for id := range ids {
			ok++
			seen = append(seen, id)
		}
		if ok != 1 || conflict != 1 {
			t.Fatalf("ok=%d conflict=%d want 1/1 ids=%v", ok, conflict, seen)
		}
	})

	t.Run("rejects nil media id", func(t *testing.T) {
		resetDB(t, pool)
		m := sampleMedia(uuid.New(), "nil-id", "body", "params")
		m.ID = uuid.Nil
		_, err := mediaRepo.InsertWithJobs(ctx, m, nil)
		if err == nil {
			t.Fatal("expected error for nil media id")
		}
	})

	t.Run("same media id different idempotency is id conflict", func(t *testing.T) {
		resetDB(t, pool)
		owner := uuid.New()
		id := uuid.New()
		first := sampleMedia(owner, "idem-a", "body-a", "params-a")
		first.ID = id
		if _, err := mediaRepo.InsertWithJobs(ctx, first, nil); err != nil {
			t.Fatal(err)
		}
		second := sampleMedia(owner, "idem-b", "body-b", "params-b")
		second.ID = id
		_, err := mediaRepo.InsertWithJobs(ctx, second, nil)
		if !errors.Is(err, ErrIDConflict) {
			t.Fatalf("got %v, want ErrIDConflict", err)
		}
	})
}

func sampleMedia(owner uuid.UUID, key, bodyFP, paramsFP string) Media {
	return Media{
		ID:                uuid.New(),
		OwnerID:           owner,
		Kind:              MediaKindImage,
		OrigFilename:      "photo.png",
		Mime:              "image/png",
		SizeBytes:         12,
		StorageKey:        owner.String() + "/" + uuid.NewString() + "/original.png",
		Metadata:          json.RawMessage(`{}`),
		IdempotencyKey:    key,
		BodyFingerprint:   bodyFP,
		ParamsFingerprint: paramsFP,
	}
}
