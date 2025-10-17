package pool

import (
    "context"
    "sync"

    "github.com/gotd/td/telegram"
    "github.com/tgdrive/teldrive/internal/logging"
    "go.uber.org/zap"
)

// Manager caches Pool instances keyed by token (bot token) or custom key.
type Manager struct {
    mu          sync.RWMutex
    pools       map[string]Pool
    clients     map[string]*telegram.Client
    size        int64
    middlewares []telegram.Middleware
}

func NewManager(size int64, middlewares []telegram.Middleware) *Manager {
    return &Manager{
        pools:       make(map[string]Pool),
        clients:     make(map[string]*telegram.Client),
        size:        size,
        middlewares: middlewares,
    }
}

// Get returns existing Pool and its underlying *telegram.Client for key, or creates them using factory.
// factory is called only when a pool does not exist yet.
func (m *Manager) Get(ctx context.Context, key string, factory func() (*telegram.Client, error), extra ...telegram.Middleware) (Pool, *telegram.Client, error) {
    m.mu.RLock()
    p, ok := m.pools[key]
    c := m.clients[key]
    m.mu.RUnlock()

    if ok && p != nil && c != nil {
        return p, c, nil
    }

    m.mu.Lock()
    defer m.mu.Unlock()

    // Double-check after acquiring write lock.
    if p, ok = m.pools[key]; ok {
        return p, m.clients[key], nil
    }

    client, err := factory()
    if err != nil {
        logging.FromContext(ctx).Error("create client for pool", zap.Error(err))
        return nil, nil, err
    }

    // combine base middlewares with extra per-request ones
    combined := make([]telegram.Middleware, 0, len(m.middlewares)+len(extra))
    combined = append(combined, m.middlewares...)
    combined = append(combined, extra...)

    newPool := NewPool(client, m.size, combined...)
    m.pools[key] = newPool
    m.clients[key] = client

    return newPool, client, nil
}

// CloseAll closes all cached pools. Errors are ignored (first error returned).
func (m *Manager) CloseAll() error {
    m.mu.Lock()
    defer m.mu.Unlock()
    var firstErr error
    for k, p := range m.pools {
        if p == nil {
            continue
        }
        if err := p.Close(); err != nil && firstErr == nil {
            firstErr = err
        }
        delete(m.pools, k)
        delete(m.clients, k)
    }
    return firstErr
}
