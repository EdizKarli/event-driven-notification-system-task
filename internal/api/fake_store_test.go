package api

import (
	"context"
	"sort"
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

func (f *fakeStore) ListNotifications(ctx context.Context, filter domain.ListFilter) ([]*domain.Notification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var matched []*domain.Notification
	for _, n := range f.items {
		if filter.Status != nil && n.Status != *filter.Status {
			continue
		}
		if filter.Channel != nil && n.Channel != *filter.Channel {
			continue
		}
		if filter.BatchID != nil && (n.BatchID == nil || *n.BatchID != *filter.BatchID) {
			continue
		}
		if filter.From != nil && n.CreatedAt.Before(*filter.From) {
			continue
		}
		if filter.To != nil && !n.CreatedAt.Before(*filter.To) {
			continue
		}
		if filter.After != nil && !isBefore(n, filter.After) {
			continue
		}
		copied := *n
		matched = append(matched, &copied)
	}

	sort.Slice(matched, func(i, j int) bool {
		if !matched[i].CreatedAt.Equal(matched[j].CreatedAt) {
			return matched[i].CreatedAt.After(matched[j].CreatedAt)
		}
		return matched[i].ID > matched[j].ID
	})

	if len(matched) > filter.Limit {
		matched = matched[:filter.Limit]
	}
	return matched, nil
}

func isBefore(n *domain.Notification, c *domain.Cursor) bool {
	if n.CreatedAt.Equal(c.CreatedAt) {
		return n.ID < c.ID
	}
	return n.CreatedAt.Before(c.CreatedAt)
}

func (f *fakeStore) CancelNotification(ctx context.Context, id string) (*domain.Notification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, ok := f.items[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	if !n.Status.CanCancel() {
		copied := *n
		return &copied, domain.ErrNotCancellable
	}
	n.Status = domain.StatusCancelled
	copied := *n
	return &copied, nil
}
