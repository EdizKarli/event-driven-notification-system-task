package api

import (
	"context"
	"sync"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
)

type fakeStore struct {
	mu        sync.Mutex
	items     map[string]*domain.Notification
	pingErr   error
	createErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{items: map[string]*domain.Notification{}}
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
