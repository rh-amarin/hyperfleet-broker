package broker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscriberErrorsChannelExists(t *testing.T) {
	// Test that the Errors() method returns a valid channel
	sub := NewMockSubscriber()
	defer func() {
		if err := sub.Close(); err != nil {
			t.Logf("failed to close subscriber: %v", err)
		}
	}()

	errChan := sub.Errors()
	assert.NotNil(t, errChan, "Errors() should return a non-nil channel")

	// Verify we can read from the channel (non-blocking check)
	select {
	case <-errChan:
		t.Fatal("should not receive error before any errors are sent")
	default:
		// Expected - channel is empty
	}
}

func TestSubscriberErrorsChannelClosedAfterClose(t *testing.T) {
	// Test that the error channel is closed when Close() is called
	sub := NewMockSubscriber()

	errChan := sub.Errors()
	require.NotNil(t, errChan)

	// Close the subscriber
	err := sub.Close()
	assert.NoError(t, err)

	// Verify channel is closed
	_, ok := <-errChan
	assert.False(t, ok, "error channel should be closed after Close()")
}

func TestMockSubscriberSimulateError(t *testing.T) {
	// Test that SimulateError() sends errors to the channel
	sub := NewMockSubscriber()
	defer func() {
		if err := sub.Close(); err != nil {
			t.Logf("failed to close subscriber: %v", err)
		}
	}()

	// Simulate an error
	testErr := &SubscriberError{
		Op:             "router",
		Topic:          "test-topic",
		SubscriptionID: "test-sub",
		Err:            fmt.Errorf("connection lost"),
		Timestamp:      time.Now(),
		Fatal:          true,
	}

	sub.SimulateError(testErr)

	// Verify error is received
	select {
	case receivedErr := <-sub.Errors():
		assert.Equal(t, "router", receivedErr.Op)
		assert.Equal(t, "test-topic", receivedErr.Topic)
		assert.Equal(t, "test-sub", receivedErr.SubscriptionID)
		assert.True(t, receivedErr.Fatal)
		assert.Contains(t, receivedErr.Err.Error(), "connection lost")
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for error")
	}
}

func TestMockSubscriberMultipleErrors(t *testing.T) {
	// Test that multiple errors can be sent and received
	sub := NewMockSubscriber()
	defer func() {
		if err := sub.Close(); err != nil {
			t.Logf("failed to close subscriber: %v", err)
		}
	}()

	// Send multiple errors
	numErrors := 5
	for i := 0; i < numErrors; i++ {
		sub.SimulateError(&SubscriberError{
			Op:             "router",
			Topic:          fmt.Sprintf("topic-%d", i),
			SubscriptionID: "test-sub",
			Err:            fmt.Errorf("error %d", i),
			Timestamp:      time.Now(),
			Fatal:          false,
		})
	}

	// Verify all errors are received
	receivedErrors := make([]*SubscriberError, 0, numErrors)
	timeout := time.After(1 * time.Second)

	for i := 0; i < numErrors; i++ {
		select {
		case err := <-sub.Errors():
			receivedErrors = append(receivedErrors, err)
		case <-timeout:
			t.Fatalf("timeout waiting for error %d", i)
		}
	}

	assert.Len(t, receivedErrors, numErrors)
	for i, err := range receivedErrors {
		assert.Equal(t, fmt.Sprintf("topic-%d", i), err.Topic)
		assert.Contains(t, err.Err.Error(), fmt.Sprintf("error %d", i))
	}
}

func TestSubscriberErrorImplementsErrorInterface(t *testing.T) {
	// Test that SubscriberError implements the error interface
	subErr := &SubscriberError{
		Op:             "router",
		Topic:          "test-topic",
		SubscriptionID: "test-sub",
		Err:            fmt.Errorf("underlying error"),
		Timestamp:      time.Now(),
		Fatal:          true,
	}

	// Verify Error() method
	errMsg := subErr.Error()
	assert.Contains(t, errMsg, "router error")
	assert.Contains(t, errMsg, "test-topic")
	assert.Contains(t, errMsg, "test-sub")
	assert.Contains(t, errMsg, "underlying error")

	// Verify Unwrap() method
	unwrapped := subErr.Unwrap()
	assert.NotNil(t, unwrapped)
	assert.Contains(t, unwrapped.Error(), "underlying error")
}

func TestMockSubscriberErrorChannelReset(t *testing.T) {
	// Test that Reset() recreates the error channel
	sub := NewMockSubscriber()

	// Send an error
	sub.SimulateError(&SubscriberError{
		Op:    "router",
		Topic: "test-topic",
		Err:   fmt.Errorf("error before reset"),
	})

	// Reset
	sub.Reset()

	// Old error should not be in the new channel
	select {
	case <-sub.Errors():
		t.Fatal("should not receive old error after reset")
	case <-time.After(100 * time.Millisecond):
		// Expected - channel is empty after reset
	}

	// Should be able to send new errors
	sub.SimulateError(&SubscriberError{
		Op:    "router",
		Topic: "test-topic-2",
		Err:   fmt.Errorf("error after reset"),
	})

	// Verify new error is received
	select {
	case err := <-sub.Errors():
		assert.Equal(t, "test-topic-2", err.Topic)
		assert.Contains(t, err.Err.Error(), "error after reset")
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for error after reset")
	}

	// Clean up
	if err := sub.Close(); err != nil {
		t.Logf("failed to close subscriber: %v", err)
	}
}

func TestSubscriberWithHandlerNoErrorsSent(t *testing.T) {
	// Test that normal operation doesn't send errors
	sub := NewMockSubscriber()
	defer func() {
		if err := sub.Close(); err != nil {
			t.Logf("failed to close subscriber: %v", err)
		}
	}()

	ctx := context.Background()

	// Create a normal handler
	handler := func(ctx context.Context, evt *event.Event) error {
		return nil
	}

	// Subscribe
	err := sub.Subscribe(ctx, "test-topic", handler)
	require.NoError(t, err)

	// Simulate a message
	evt := event.New()
	evt.SetID("test-id")
	evt.SetType("test.type")
	evt.SetSource("test-source")

	err = sub.SimulateMessage(ctx, "test-topic", &evt)
	assert.NoError(t, err)

	// Verify no errors were sent to the channel
	select {
	case err := <-sub.Errors():
		t.Fatalf("unexpected error received: %v", err)
	case <-time.After(100 * time.Millisecond):
		// Expected - no errors
	}
}

