package apiserver

import (
	"context"
	"sync"
)

// documentCache holds an OpenAPI document that was expensive to obtain.
//
// Both documents are rebuilt from kcp on demand: v3 is a round trip per fetch,
// and v2 lists every CustomResourceDefinition in the workspace and builds a
// specification from each. The aggregation layer polls both on its own
// schedule, for as long as spillway is registered, so without a cache that cost
// is paid forever whether or not anything changed.
//
// The entry is keyed on the resource cache's generation rather than on a timer.
// That covers a schema being edited as well as a group version appearing or
// disappearing: refreshes are driven by a watch on the workspace's CRDs, so any
// change to one advances the generation and the document is rebuilt on the next
// request. A timer would instead leave a window in which a document that no
// longer matches what spillway serves is handed out anyway.
type documentCache struct {
	// build produces the document. It is called with the caller's context, so a
	// slow kcp is bounded by the request that triggered the rebuild.
	build func(ctx context.Context) ([]byte, error)

	mu       sync.Mutex
	document []byte
	// generation is the API surface the cached document was built from. When it
	// differs from the current one the document is rebuilt.
	generation uint64
	built      bool
}

func newDocumentCache(build func(ctx context.Context) ([]byte, error)) *documentCache {
	return &documentCache{build: build}
}

// get returns the document for the given generation, building it if what is
// held was built from a different one.
func (c *documentCache) get(ctx context.Context, generation uint64) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.built && c.generation == generation {
		return c.document, nil
	}

	document, err := c.build(ctx)
	if err != nil {
		// A failed rebuild does not discard what is held: serving the previous
		// answer beats serving none while kcp is briefly unreachable. It stays
		// marked as built from the older generation, so the next request tries
		// again.
		if c.built {
			return c.document, nil
		}
		return nil, err
	}

	c.document = document
	c.generation = generation
	c.built = true
	return document, nil
}

// invalidate forgets the cached document. Used when the workspace's API surface
// changes in a way the generation alone does not capture.
func (c *documentCache) invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.built = false
}
