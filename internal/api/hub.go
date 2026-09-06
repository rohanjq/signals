package api

import (
	"sort"
	"sync"

	"github.com/rohanjq/signals/internal/model"
)

type Subscription struct {
	Series      []seriesSelector
	Analyses    []analysisSelector
	EventTypes  map[string]struct{}
	Confirmed   bool
	Provisional bool
}

type seriesSelector struct {
	Dataset   string `json:"dataset,omitempty"`
	Symbol    string `json:"symbol"`
	Timeframe string `json:"timeframe"`
}

type analysisSelector struct {
	Name       string `json:"name"`
	ConfigHash string `json:"config_hash,omitempty"`
}

type Hub struct {
	mu      sync.RWMutex
	clients map[*hubClient]struct{}
}

type hubClient struct {
	mu         sync.Mutex
	filter     Subscription
	send       chan model.SignalEvent
	active     bool
	pending    []model.SignalEvent
	lastCursor int64
	closed     bool
}

func NewHub() *Hub { return &Hub{clients: make(map[*hubClient]struct{})} }

func (h *Hub) Publish(event model.SignalEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for client := range h.clients {
		client.enqueue(event)
	}
}

func (h *Hub) subscribe(filter Subscription, buffer int) *hubClient {
	if buffer <= 0 {
		buffer = 1024
	}
	client := &hubClient{filter: filter, send: make(chan model.SignalEvent, buffer)}
	h.mu.Lock()
	h.clients[client] = struct{}{}
	h.mu.Unlock()
	return client
}

func (h *Hub) unsubscribe(client *hubClient) {
	h.mu.Lock()
	delete(h.clients, client)
	h.mu.Unlock()
	client.close()
}

func (c *hubClient) enqueue(event model.SignalEvent) {
	if !matches(c.filter, event) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	if event.Cursor > 0 && event.Cursor <= c.lastCursor {
		return
	}
	if !c.active {
		if len(c.pending) >= cap(c.send) {
			c.closed = true
			close(c.send)
			return
		}
		c.pending = append(c.pending, event)
		return
	}
	select {
	case c.send <- event:
		if event.Cursor > 0 {
			c.lastCursor = event.Cursor
		}
	default:
		c.closed = true
		close(c.send)
	}
}

func (c *hubClient) activate(afterCursor int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	sort.SliceStable(c.pending, func(i, j int) bool {
		left, right := c.pending[i].Cursor, c.pending[j].Cursor
		if left == 0 {
			return false
		}
		if right == 0 {
			return true
		}
		return left < right
	})
	for _, event := range c.pending {
		if event.Cursor > 0 && event.Cursor <= afterCursor {
			continue
		}
		select {
		case c.send <- event:
			if event.Cursor > 0 {
				c.lastCursor = event.Cursor
			}
		default:
			if !c.closed {
				c.closed = true
				close(c.send)
			}
			return
		}
	}
	c.pending = nil
	if afterCursor > c.lastCursor {
		c.lastCursor = afterCursor
	}
	c.active = true
}

func (c *hubClient) close() {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		close(c.send)
	}
	c.mu.Unlock()
}

func matches(filter Subscription, event model.SignalEvent) bool {
	if event.Provisional && !filter.Provisional {
		return false
	}
	if !event.Provisional && !filter.Confirmed {
		return false
	}
	if len(filter.Series) > 0 && !matchesSeries(filter.Series, event.Dataset, event.Symbol, event.Timeframe) {
		return false
	}
	if len(filter.EventTypes) > 0 {
		if _, ok := filter.EventTypes[analysisEventType]; !ok {
			return false
		}
	}
	if len(filter.Analyses) > 0 && !matchesAnalysis(filter.Analyses, event.Algorithm) {
		return false
	}
	return true
}

func matchesAnalysis(selectors []analysisSelector, algorithm model.AlgorithmRef) bool {
	for _, selector := range selectors {
		if (selector.Name == "*" || selector.Name == algorithm.Name) &&
			(selector.ConfigHash == "" || selector.ConfigHash == algorithm.ConfigHash) {
			return true
		}
	}
	return false
}

func matchesSeries(selectors []seriesSelector, dataset, symbol, timeframe string) bool {
	for _, selector := range selectors {
		if selector.Symbol == symbol && selector.Timeframe == timeframe &&
			(selector.Dataset == "" || selector.Dataset == dataset) {
			return true
		}
	}
	return false
}
