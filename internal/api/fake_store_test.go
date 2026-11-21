package api

import (
	"context"
	"sync"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
)

type fakeStore struct {
	mu        sync.Mutex
	items     map[string]*domain.Notification
	batches   map[string]*domain.Batch
	pingErr   error
	createErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		items:   map[string]*domain.Notification{},
		batches: map[string]*domain.Batch{},
	}
}

func (f *fakeStore) Ping(ctx context.Context) error {
	return f.pingErr
}

func (f *fakeStore) CreateNotification(ctx context.Context, n *domain.Notification) error {
	if f.createErr != nil {
		return f.createErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	stored := *n
	f.items[n.ID] = &stored
	return nil
}

func (f *fakeStore) GetNotification(ctx context.Context, id string) (*domain.Notification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, ok := f.items[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	copied := *n
	return &copied, nil
}

func (f *fakeStore) CreateBatch(ctx context.Context, batch *domain.Batch, items []*domain.Notification) error {
	if f.createErr != nil {
		return f.createErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	stored := *batch
	f.batches[batch.ID] = &stored
	for _, n := range items {
		copied := *n
		f.items[n.ID] = &copied
	}
	return nil
}

func (f *fakeStore) GetBatch(ctx context.Context, id string) (*domain.Batch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.batches[id]
	if !ok {
		return nil, domain.ErrBatchNotFound
	}
	result := *b
	result.StatusCounts = map[domain.Status]int{}
	for _, n := range f.items {
		if n.BatchID != nil && *n.BatchID == id {
			result.StatusCounts[n.Status]++
		}
	}
	return &result, nil
}
