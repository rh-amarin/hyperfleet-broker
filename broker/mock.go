package broker

import (
	"context"
	"fmt"
	"sync"

	"github.com/cloudevents/sdk-go/v2/event"
)

// MockPublisher is a mock implementation of Publisher for testing purposes
type MockPublisher struct {
	mu sync.Mutex

	// PublishedEvents stores all events that have been published, keyed by topic
	PublishedEvents map[string][]*event.Event

	// PublishError is returned by Publish if set
	PublishError error

	// PublishFunc is called on Publish if set, allowing custom behavior
	PublishFunc func(topic string, event *event.Event) error

	// CloseError is returned by Close if set
	CloseError error

	// Closed indicates if Close was called
	Closed bool
}

// NewMockPublisher creates a new MockPublisher
func NewMockPublisher() *MockPublisher {
	return &MockPublisher{
		PublishedEvents: make(map[string][]*event.Event),
	}
}

// Publish implements Publisher.Publish
func (m *MockPublisher) Publish(topic string, evt *event.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.PublishFunc != nil {
		return m.PublishFunc(topic, evt)
	}

	if m.PublishError != nil {
		return m.PublishError
	}

	m.PublishedEvents[topic] = append(m.PublishedEvents[topic], evt)
	return nil
}

// Close implements Publisher.Close
func (m *MockPublisher) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Closed = true
	return m.CloseError
}

// GetPublishedEvents returns all events published to a given topic
func (m *MockPublisher) GetPublishedEvents(topic string) []*event.Event {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.PublishedEvents[topic]
}

// GetAllPublishedEvents returns all events published to all topics
func (m *MockPublisher) GetAllPublishedEvents() map[string][]*event.Event {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Return a copy to avoid race conditions
	result := make(map[string][]*event.Event)
	for k, v := range m.PublishedEvents {
		result[k] = append([]*event.Event{}, v...)
	}
	return result
}

// Reset clears all published events and resets state
func (m *MockPublisher) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.PublishedEvents = make(map[string][]*event.Event)
	m.PublishError = nil
	m.CloseError = nil
	m.Closed = false
	m.PublishFunc = nil
}

// MockSubscriber is a mock implementation of Subscriber for testing purposes
type MockSubscriber struct {
	mu sync.Mutex

	// Handlers stores all registered handlers, keyed by topic
	Handlers map[string][]HandlerFunc

	// SubscribeError is returned by Subscribe if set
	SubscribeError error

	// SubscribeFunc is called on Subscribe if set, allowing custom behavior
	SubscribeFunc func(ctx context.Context, topic string, handler HandlerFunc) error

	// CloseError is returned by Close if set
	CloseError error

	// Closed indicates if Close was called
	Closed bool

	// EventQueue stores events to be delivered to handlers
	eventQueue chan *mockEvent
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup

	// Error channel for error notifications
	errorChan chan *SubscriberError
}

type mockEvent struct {
	topic string
	event *event.Event
}

// NewMockSubscriber creates a new MockSubscriber
func NewMockSubscriber() *MockSubscriber {
	return &MockSubscriber{
		Handlers:   make(map[string][]HandlerFunc),
		eventQueue: make(chan *mockEvent, 100),
		errorChan:  make(chan *SubscriberError, 100),
	}
}

// Subscribe implements Subscriber.Subscribe
func (m *MockSubscriber) Subscribe(ctx context.Context, topic string, handler HandlerFunc) error {
	if handler == nil {
		return fmt.Errorf("handler must be provided")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.SubscribeFunc != nil {
		return m.SubscribeFunc(ctx, topic, handler)
	}

	if m.SubscribeError != nil {
		return m.SubscribeError
	}

	m.Handlers[topic] = append(m.Handlers[topic], handler)
	return nil
}

// Errors implements Subscriber.Errors
func (m *MockSubscriber) Errors() <-chan *SubscriberError {
	return m.errorChan
}

// SimulateError allows tests to inject errors into the error channel
func (m *MockSubscriber) SimulateError(err *SubscriberError) {
	m.errorChan <- err
}

// Close implements Subscriber.Close
func (m *MockSubscriber) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Closed = true
	if m.cancelFunc != nil {
		m.cancelFunc()
	}
	close(m.eventQueue)
	close(m.errorChan)
	m.wg.Wait()
	return m.CloseError
}

// SimulateMessage simulates receiving a message on a topic and delivers it to all handlers
// Returns an error if no handlers are registered for the topic or if any handler returns an error
func (m *MockSubscriber) SimulateMessage(ctx context.Context, topic string, evt *event.Event) error {
	m.mu.Lock()
	handlers := m.Handlers[topic]
	m.mu.Unlock()

	if len(handlers) == 0 {
		return fmt.Errorf("no handlers registered for topic: %s", topic)
	}

	var errs []error
	for _, handler := range handlers {
		if err := handler(ctx, evt); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("handler errors: %v", errs)
	}

	return nil
}

// GetHandlers returns all handlers registered for a given topic
func (m *MockSubscriber) GetHandlers(topic string) []HandlerFunc {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.Handlers[topic]
}

// HasHandler checks if there's at least one handler registered for the topic
func (m *MockSubscriber) HasHandler(topic string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.Handlers[topic]) > 0
}

// Reset clears all handlers and resets state
func (m *MockSubscriber) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Handlers = make(map[string][]HandlerFunc)
	m.SubscribeError = nil
	m.CloseError = nil
	m.Closed = false
	m.SubscribeFunc = nil
	m.errorChan = make(chan *SubscriberError, 100)
}

// Ensure mock implementations satisfy the interfaces
var _ Publisher = (*MockPublisher)(nil)
var _ Subscriber = (*MockSubscriber)(nil)
