package apiserver

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func countingBuild(calls *int, fail error) func(context.Context) ([]byte, error) {
	return func(context.Context) ([]byte, error) {
		*calls++
		if fail != nil {
			return nil, fail
		}
		return fmt.Appendf(nil, "document %d", *calls), nil
	}
}

// The aggregation layer polls these documents for as long as spillway is
// registered. Rebuilding per poll is the cost this exists to remove.
func TestCacheBuildsOnceForAGeneration(t *testing.T) {
	var calls int
	cache := newDocumentCache(countingBuild(&calls, nil))

	for range 5 {
		document, err := cache.get(context.Background(), 7)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if string(document) != "document 1" {
			t.Errorf("got %q, want the first build to be reused", document)
		}
	}
	if calls != 1 {
		t.Errorf("built %d times for one generation, want 1", calls)
	}
}

// A document that no longer matches what spillway serves is wrong, not stale.
func TestCacheRebuildsWhenTheGenerationMoves(t *testing.T) {
	var calls int
	cache := newDocumentCache(countingBuild(&calls, nil))

	first, _ := cache.get(context.Background(), 1)
	second, _ := cache.get(context.Background(), 2)

	if string(first) == string(second) {
		t.Error("the document was reused across generations")
	}
	if calls != 2 {
		t.Errorf("built %d times across two generations, want 2", calls)
	}
}

// Serving the previous answer beats serving none while kcp is briefly away.
func TestCacheServesTheOldDocumentWhenTheRebuildFails(t *testing.T) {
	var calls int
	build := func(ctx context.Context) ([]byte, error) {
		calls++
		if calls == 1 {
			return []byte("good"), nil
		}
		return nil, errors.New("kcp is unreachable")
	}
	cache := newDocumentCache(build)

	if _, err := cache.get(context.Background(), 1); err != nil {
		t.Fatalf("first get: %v", err)
	}

	document, err := cache.get(context.Background(), 2)
	if err != nil {
		t.Fatalf("a failed rebuild surfaced instead of the previous document: %v", err)
	}
	if string(document) != "good" {
		t.Errorf("got %q, want the previously built document", document)
	}
}

// With nothing held there is nothing to fall back to, and the caller has to
// hear about it.
func TestCacheReportsAFailureWithNothingCached(t *testing.T) {
	var calls int
	cache := newDocumentCache(countingBuild(&calls, errors.New("kcp is unreachable")))

	if _, err := cache.get(context.Background(), 1); err == nil {
		t.Error("a failure with nothing cached was hidden")
	}
}

// A failed rebuild must not mark the new generation as done, or the stale
// document would be served until the generation moves again.
func TestCacheRetriesAfterAFailedRebuild(t *testing.T) {
	var calls int
	build := func(ctx context.Context) ([]byte, error) {
		calls++
		switch calls {
		case 1:
			return []byte("first"), nil
		case 2:
			return nil, errors.New("kcp is unreachable")
		default:
			return []byte("second"), nil
		}
	}
	cache := newDocumentCache(build)

	_, _ = cache.get(context.Background(), 1)
	_, _ = cache.get(context.Background(), 2) // fails, serves "first"

	document, err := cache.get(context.Background(), 2)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(document) != "second" {
		t.Errorf("got %q, want the retry to have rebuilt", document)
	}
}

func TestCacheInvalidate(t *testing.T) {
	var calls int
	cache := newDocumentCache(countingBuild(&calls, nil))

	_, _ = cache.get(context.Background(), 1)
	cache.invalidate()
	_, _ = cache.get(context.Background(), 1)

	if calls != 2 {
		t.Errorf("built %d times, want a rebuild after invalidate", calls)
	}
}
