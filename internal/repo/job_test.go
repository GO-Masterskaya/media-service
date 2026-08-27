package repo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestCanTransition(t *testing.T) {
	tests := []struct {
		from, to string
		want     bool
	}{
		{"queued", "running", true},
		{"running", "done", true},
		{"running", "failed", true},
		{"running", "queued", true},
		{"queued", "done", false},
		{"queued", "failed", false},
		{"done", "running", false},
		{"failed", "running", false},
		{"done", "queued", false},
	}
	for _, tt := range tests {
		t.Run(tt.from+"->"+tt.to, func(t *testing.T) {
			if got := CanTransition(tt.from, tt.to); got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJobRepo(t *testing.T) {
	pool := setupPostgres(t)
	jobs := NewPgJobRepo(pool)
	mediaRepo := NewPgMediaRepo(pool)
	derivs := NewPgDerivativeRepo(pool)
	ctx := context.Background()

	t.Run("claim sets running lease", func(t *testing.T) {
		resetDB(t, pool)
		mediaID := seedMedia(t, pool)
		jobID := seedJob(t, pool, mediaID, "thumbnail", "queued")

		const owner = "worker-1"
		claimedAt := time.Now()
		job, err := jobs.ClaimNext(ctx, owner, DefaultJobLease)
		if err != nil {
			t.Fatal(err)
		}
		if job.ID != jobID {
			t.Fatalf("claimed %v, want %v", job.ID, jobID)
		}
		if job.Status != JobStatusRunning {
			t.Fatalf("status %s, want running", job.Status)
		}
		if job.LockedBy != owner {
			t.Fatalf("locked_by %q, want %q", job.LockedBy, owner)
		}
		if !job.LeaseUntil.After(claimedAt) {
			t.Fatalf("lease_until %s must be after claim time %s", job.LeaseUntil, claimedAt)
		}
	})

	t.Run("parallel claimers get distinct jobs", func(t *testing.T) {
		resetDB(t, pool)
		mediaID := seedMedia(t, pool)
		seedJob(t, pool, mediaID, "thumbnail", "queued")

		var wg sync.WaitGroup
		type result struct {
			job *Job
			err error
		}
		out := make(chan result, 2)
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(owner string) {
				defer wg.Done()
				job, err := jobs.ClaimNext(ctx, owner, DefaultJobLease)
				out <- result{job, err}
			}(fmt.Sprintf("worker-%d", i))
		}
		wg.Wait()
		close(out)

		var got []*Job
		var notFound int
		for r := range out {
			if r.err != nil {
				if errors.Is(r.err, ErrNotFound) {
					notFound++
					continue
				}
				t.Fatalf("unexpected error: %v", r.err)
			}
			got = append(got, r.job)
		}
		if len(got) != 1 || notFound != 1 {
			t.Fatalf("got %d claims and %d not found, want 1 and 1", len(got), notFound)
		}
	})

	t.Run("enqueue is idempotent", func(t *testing.T) {
		resetDB(t, pool)
		mediaID := seedMedia(t, pool)
		first, err := jobs.Enqueue(ctx, mediaID, "thumbnail")
		if err != nil {
			t.Fatal(err)
		}
		second, err := jobs.Enqueue(ctx, mediaID, "thumbnail")
		if err != nil {
			t.Fatal(err)
		}
		if first.ID != second.ID {
			t.Fatalf("enqueue returned %v, want existing %v", second.ID, first.ID)
		}
	})

	t.Run("enqueue concurrent same intent", func(t *testing.T) {
		resetDB(t, pool)
		mediaID := seedMedia(t, pool)
		var wg sync.WaitGroup
		ids := make(chan uuid.UUID, 2)
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				job, err := jobs.Enqueue(ctx, mediaID, "transcode")
				if err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
				ids <- job.ID
			}()
		}
		wg.Wait()
		close(ids)
		var seen []uuid.UUID
		for id := range ids {
			seen = append(seen, id)
		}
		if len(seen) != 2 || seen[0] != seen[1] {
			t.Fatalf("concurrent enqueue ids %v, want the same job twice", seen)
		}
	})

	t.Run("enqueue different types", func(t *testing.T) {
		resetDB(t, pool)
		mediaID := seedMedia(t, pool)
		a, err := jobs.Enqueue(ctx, mediaID, "thumbnail")
		if err != nil {
			t.Fatal(err)
		}
		b, err := jobs.Enqueue(ctx, mediaID, "transcode")
		if err != nil {
			t.Fatal(err)
		}
		if a.ID == b.ID {
			t.Fatal("different job types must create different jobs")
		}
	})

	t.Run("mark done wrong owner", func(t *testing.T) {
		resetDB(t, pool)
		mediaID := seedMedia(t, pool)
		jobID := seedJob(t, pool, mediaID, "thumbnail", "queued")
		job, err := jobs.ClaimNext(ctx, "worker-1", DefaultJobLease)
		if err != nil {
			t.Fatal(err)
		}
		if job.ID != jobID {
			t.Fatalf("claimed %v, want %v", job.ID, jobID)
		}
		err = jobs.MarkDone(ctx, job.ID, "worker-other")
		if !errors.Is(err, ErrLeaseMismatch) {
			t.Fatalf("got %v, want ErrLeaseMismatch", err)
		}
	})

	t.Run("mark done not last keeps processing", func(t *testing.T) {
		resetDB(t, pool)
		mediaID := seedMedia(t, pool)
		seedJob(t, pool, mediaID, "thumbnail", "queued")
		seedJob(t, pool, mediaID, "transcode", "queued")

		job, err := jobs.ClaimNext(ctx, "worker-1", DefaultJobLease)
		if err != nil {
			t.Fatal(err)
		}
		if err := jobs.MarkDone(ctx, job.ID, "worker-1"); err != nil {
			t.Fatal(err)
		}
		m, err := mediaRepo.GetByID(ctx, mediaID)
		if err != nil {
			t.Fatal(err)
		}
		if m.Status != MediaStatusProcessing {
			t.Fatalf("status %s, want processing", m.Status)
		}
	})

	t.Run("mark done last job sets ready", func(t *testing.T) {
		resetDB(t, pool)
		mediaID := seedMedia(t, pool)
		seedJob(t, pool, mediaID, "thumbnail", "queued")

		job, err := jobs.ClaimNext(ctx, "worker-1", DefaultJobLease)
		if err != nil {
			t.Fatal(err)
		}
		if err := jobs.MarkDone(ctx, job.ID, "worker-1"); err != nil {
			t.Fatal(err)
		}
		m, err := mediaRepo.GetByID(ctx, mediaID)
		if err != nil {
			t.Fatal(err)
		}
		if m.Status != MediaStatusReady {
			t.Fatalf("status %s, want ready", m.Status)
		}
	})

	t.Run("concurrent last jobs set ready once", func(t *testing.T) {
		resetDB(t, pool)
		mediaID := seedMedia(t, pool)
		seedJob(t, pool, mediaID, "thumbnail", "queued")
		seedJob(t, pool, mediaID, "transcode", "queued")

		j1, err := jobs.ClaimNext(ctx, "worker-1", DefaultJobLease)
		if err != nil {
			t.Fatal(err)
		}
		j2, err := jobs.ClaimNext(ctx, "worker-2", DefaultJobLease)
		if err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		errs := make(chan error, 2)
		wg.Add(2)
		go func() {
			defer wg.Done()
			errs <- jobs.MarkDone(ctx, j1.ID, "worker-1")
		}()
		go func() {
			defer wg.Done()
			errs <- jobs.MarkDone(ctx, j2.ID, "worker-2")
		}()
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}

		m, err := mediaRepo.GetByID(ctx, mediaID)
		if err != nil {
			t.Fatal(err)
		}
		if m.Status != MediaStatusReady {
			t.Fatalf("status %s, want ready", m.Status)
		}
	})

	t.Run("mark failed sets media failed", func(t *testing.T) {
		resetDB(t, pool)
		mediaID := seedMedia(t, pool)
		seedJob(t, pool, mediaID, "thumbnail", "queued")
		job, err := jobs.ClaimNext(ctx, "worker-1", DefaultJobLease)
		if err != nil {
			t.Fatal(err)
		}
		const reason = "ffmpeg exit 1"
		if err := jobs.MarkFailed(ctx, job.ID, "worker-1", reason); err != nil {
			t.Fatal(err)
		}
		m, err := mediaRepo.GetByID(ctx, mediaID)
		if err != nil {
			t.Fatal(err)
		}
		if m.Status != MediaStatusFailed {
			t.Fatalf("status %s, want failed", m.Status)
		}
		if m.Error != reason {
			t.Fatalf("error %q, want %q", m.Error, reason)
		}
	})

	t.Run("invalid complete from queued", func(t *testing.T) {
		resetDB(t, pool)
		mediaID := seedMedia(t, pool)
		jobID := seedJob(t, pool, mediaID, "thumbnail", "queued")
		err := jobs.MarkDone(ctx, jobID, "worker-1")
		if !errors.Is(err, ErrInvalidTransition) && !errors.Is(err, ErrLeaseMismatch) {
			t.Fatalf("got %v, want invalid transition or lease mismatch", err)
		}
	})

	t.Run("release returns job to queued", func(t *testing.T) {
		resetDB(t, pool)
		mediaID := seedMedia(t, pool)
		seedJob(t, pool, mediaID, "thumbnail", "queued")
		job, err := jobs.ClaimNext(ctx, "worker-1", DefaultJobLease)
		if err != nil {
			t.Fatal(err)
		}
		if err := jobs.Release(ctx, job.ID, "worker-1"); err != nil {
			t.Fatal(err)
		}
		again, err := jobs.ClaimNext(ctx, "worker-2", DefaultJobLease)
		if err != nil {
			t.Fatal(err)
		}
		if again.ID != job.ID {
			t.Fatalf("reclaimed %v, want %v", again.ID, job.ID)
		}
		if again.LockedBy != "worker-2" {
			t.Fatalf("locked_by %q, want worker-2", again.LockedBy)
		}
	})

	t.Run("insert derivative dedup", func(t *testing.T) {
		resetDB(t, pool)
		mediaID := seedMedia(t, pool)
		in := Derivative{
			MediaID:    mediaID,
			Variant:    "thumbnail",
			Mime:       "image/jpeg",
			SizeBytes:  10,
			StorageKey: "k/thumb.jpg",
		}
		first, err := derivs.Insert(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		in.StorageKey = "k/other.jpg"
		second, err := derivs.Insert(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		if first.ID != second.ID {
			t.Fatalf("derivative ids %v and %v, want same", first.ID, second.ID)
		}
		if second.StorageKey != first.StorageKey {
			t.Fatalf("storage key changed to %s", second.StorageKey)
		}
	})

	t.Run("insert derivative concurrent", func(t *testing.T) {
		resetDB(t, pool)
		mediaID := seedMedia(t, pool)
		var wg sync.WaitGroup
		ids := make(chan uuid.UUID, 2)
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				d, err := derivs.Insert(ctx, Derivative{
					MediaID:    mediaID,
					Variant:    "r_720",
					Mime:       "video/mp4",
					SizeBytes:  20,
					StorageKey: "k/r720.mp4",
				})
				if err != nil {
					t.Errorf("insert: %v", err)
					return
				}
				ids <- d.ID
			}()
		}
		wg.Wait()
		close(ids)
		var seen []uuid.UUID
		for id := range ids {
			seen = append(seen, id)
		}
		if len(seen) != 2 || seen[0] != seen[1] {
			t.Fatalf("concurrent insert ids %v, want the same derivative twice", seen)
		}
	})
}

func setupPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()

	ctx := context.Background()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		dsn = startPostgres(t, ctx)
	}

	if err := RunMigrations(dsn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	pool, err := NewPool(ctx, PoolConfig{DSN: dsn})
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	t.Cleanup(pool.Close)

	return pool
}

func startPostgres(t *testing.T, ctx context.Context) string {
	t.Helper()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16-alpine",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     "media",
				"POSTGRES_PASSWORD": "media",
				"POSTGRES_DB":       "media",
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("postgres host: %v", err)
	}
	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("postgres port: %v", err)
	}

	return fmt.Sprintf("postgres://media:media@%s:%s/media?sslmode=disable", host, port.Port())
}

func resetDB(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `TRUNCATE media CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

func seedMedia(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()

	id := uuid.New()
	ownerID := uuid.New()
	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		INSERT INTO media (
			id,
			owner_id,
			kind,
			orig_filename,
			mime,
			size_bytes,
			status,
			storage_key,
			idempotency_key
		)
		VALUES (
			$1,
			$2,
			'image',
			'test.jpg',
			'image/jpeg',
			100,
			'processing',
			'test/test.jpg',
			$3
		)
	`, id, ownerID, uuid.NewString())
	if err != nil {
		t.Fatalf("seed media: %v", err)
	}

	return id
}

func seedJob(t *testing.T, pool *pgxpool.Pool, mediaID uuid.UUID, jobType, status string) uuid.UUID {
	t.Helper()

	id := uuid.New()
	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		INSERT INTO processing_jobs (id, media_id, type, status)
		VALUES ($1, $2, $3, $4)
	`, id, mediaID, jobType, status)
	if err != nil {
		t.Fatalf("seed job: %v", err)
	}

	return id
}
