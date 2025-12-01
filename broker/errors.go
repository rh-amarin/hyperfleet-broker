package broker

import (
	"fmt"
	"time"
)

// SubscriberError represents an error that occurred during message processing
// in background goroutines. These errors are sent via the Errors() channel.
type SubscriberError struct {
	// Op is the operation that failed: "router", "connect", "receive"
	Op string

	// Topic where the error occurred
	Topic string

	// SubscriptionID of the subscriber
	SubscriptionID string

	// Err is the underlying error
	Err error

	// Timestamp is when the error occurred
	Timestamp time.Time

	// Fatal indicates if the subscriber has stopped and cannot continue operating.
	// If true, the subscriber needs to be recreated.
	Fatal bool
}

// Error implements the error interface
func (e *SubscriberError) Error() string {
	return fmt.Sprintf("%s error on topic %q (subscription: %s): %v", e.Op, e.Topic, e.SubscriptionID, e.Err)
}

// Unwrap implements the errors.Unwrap interface
func (e *SubscriberError) Unwrap() error {
	return e.Err
}

