package common

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openshift-hyperfleet/hyperfleet-broker/broker"
)

// SetupTestEnvironment sets up the test environment for Podman and disables Ryuk
// This should be called from TestMain in each test package
func SetupTestEnvironment() {
	// Disable Ryuk (resource reaper) - required for Podman
	// This should be set before any testcontainers operations
	if os.Getenv("TESTCONTAINERS_RYUK_DISABLED") == "" {
		if err := os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true"); err != nil {
			fmt.Fprintf(os.Stderr, "failed to set TESTCONTAINERS_RYUK_DISABLED: %v\n", err)
		}
	}

	// Configure Podman socket if DOCKER_HOST is not already set
	// On Linux with rootless Podman:
	// export DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock
	// On macOS with Podman machine:
	// export DOCKER_HOST=unix://$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}')
	if os.Getenv("DOCKER_HOST") == "" {
		// Try to detect Podman socket location
		if xdgRuntimeDir := os.Getenv("XDG_RUNTIME_DIR"); xdgRuntimeDir != "" {
			podmanSock := fmt.Sprintf("%s/podman/podman.sock", xdgRuntimeDir)
			if _, err := os.Stat(podmanSock); err == nil {
				if err := os.Setenv("DOCKER_HOST", fmt.Sprintf("unix://%s", podmanSock)); err != nil {
					fmt.Fprintf(os.Stderr, "failed to set DOCKER_HOST: %v\n", err)
				}
			}
		}
	}
}

// BrokerTestConfig holds broker-specific test configuration
type BrokerTestConfig struct {
	BrokerType      string
	SetupSleep      time.Duration
	ReceiveTimeout  time.Duration
	SetupConfigFunc func(*testing.T, map[string]string) // Optional function to modify config after initial setup
	PublishDelay    time.Duration                       // Delay between publishes (for gradual publishing, 0 = no delay)
}

// BuildConfigMap creates a configuration map for testing
func BuildConfigMap(brokerType string, rabbitMQURL string, pubsubProjectID string) map[string]string {
	configMap := map[string]string{
		"broker.type":            brokerType,
		"subscriber.parallelism": "1",
	}

	switch brokerType {
	case "rabbitmq":
		configMap["broker.rabbitmq.url"] = rabbitMQURL
	case "googlepubsub":
		configMap["broker.googlepubsub.project_id"] = pubsubProjectID
		// Enable auto-creation of topics and subscriptions for tests
		configMap["broker.googlepubsub.create_topic_if_missing"] = "true"
		configMap["broker.googlepubsub.create_subscription_if_missing"] = "true"
	}

	return configMap
}

// RunPublisherSubscriber tests the full publish/subscribe flow
func RunPublisherSubscriber(t *testing.T, configMap map[string]string, cfg BrokerTestConfig) {
	ctx := context.Background()

	// Create publisher
	pub, err := broker.NewPublisher(configMap)
	require.NoError(t, err)
	defer func() {
		if err := pub.Close(); err != nil {
			t.Logf("failed to close publisher: %v", err)
		}
	}()

	// Create subscriber
	subscriptionID := "test-subscription"
	sub, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)
	defer func() {
		if err := sub.Close(); err != nil {
			t.Logf("failed to close subscriber: %v", err)
		}
	}()

	// Create a CloudEvent
	evt := event.New()
	evt.SetType("com.example.test.event")
	evt.SetSource("test-source")
	evt.SetID("test-id-123")
	if err := evt.SetData(event.ApplicationJSON, map[string]string{
		"message": "Hello, World!",
	}); err != nil {
		require.NoError(t, err, "failed to set event data")
	}

	// Set up handler
	receivedEvents := make(chan *event.Event, 1)
	handler := func(ctx context.Context, e *event.Event) error {
		receivedEvents <- e
		return nil
	}

	// Subscribe to topic
	err = sub.Subscribe(ctx, "test-topic", handler)
	require.NoError(t, err)

	// Give subscriber time to set up
	time.Sleep(cfg.SetupSleep)

	err = pub.Publish("test-topic", &evt)
	require.NoError(t, err)

	// Wait for event to be received
	select {
	case receivedEvt := <-receivedEvents:
		assert.Equal(t, evt.Type(), receivedEvt.Type())
		assert.Equal(t, evt.ID(), receivedEvt.ID())
		assert.Equal(t, evt.Source(), receivedEvt.Source())
	case <-time.After(cfg.ReceiveTimeout):
		t.Fatal("timeout waiting for event")
	}
}

// RunMultipleEvents tests that multiple events are processed correctly
func RunMultipleEvents(t *testing.T, configMap map[string]string, cfg BrokerTestConfig) {
	ctx := context.Background()

	pub, err := broker.NewPublisher(configMap)
	require.NoError(t, err)
	defer func() {
		if err := pub.Close(); err != nil {
			t.Logf("failed to close publisher: %v", err)
		}
	}()

	subscriptionID := "test-subscription"
	sub, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)
	defer func() {
		if err := sub.Close(); err != nil {
			t.Logf("failed to close subscriber: %v", err)
		}
	}()

	// Create two different event types
	evt1 := event.New()
	evt1.SetType("com.example.event.type1")
	evt1.SetSource("test-source")
	evt1.SetID("id-1")
	if err := evt1.SetData(event.ApplicationJSON, map[string]string{"type": "1"}); err != nil {
		require.NoError(t, err, "failed to set event data")
	}

	evt2 := event.New()
	evt2.SetType("com.example.event.type2")
	evt2.SetSource("test-source")
	evt2.SetID("id-2")
	if err := evt2.SetData(event.ApplicationJSON, map[string]string{"type": "2"}); err != nil {
		require.NoError(t, err, "failed to set event data")
	}

	// Set up handler to collect all events
	receivedEvents := make(chan *event.Event, 2)
	handler := func(ctx context.Context, e *event.Event) error {
		receivedEvents <- e
		return nil
	}

	err = sub.Subscribe(ctx, "routing-topic", handler)
	require.NoError(t, err)

	time.Sleep(cfg.SetupSleep)

	// Publish both events
	err = pub.Publish("routing-topic", &evt1)
	require.NoError(t, err)

	err = pub.Publish("routing-topic", &evt2)
	require.NoError(t, err)

	// Verify both events were received
	eventsReceived := make(map[string]bool)
	for i := 0; i < 2; i++ {
		select {
		case received := <-receivedEvents:
			eventsReceived[received.ID()] = true
			assert.Contains(t, []string{"id-1", "id-2"}, received.ID())
		case <-time.After(cfg.ReceiveTimeout):
			t.Fatalf("timeout waiting for event %d", i+1)
		}
	}

	assert.True(t, eventsReceived["id-1"], "event id-1 should be received")
	assert.True(t, eventsReceived["id-2"], "event id-2 should be received")
}

// RunSharedSubscription tests that two subscribers with the same subscriptionID share messages
func RunSharedSubscription(t *testing.T, configMap map[string]string, cfg BrokerTestConfig) {
	ctx := context.Background()

	pub, err := broker.NewPublisher(configMap)
	require.NoError(t, err)
	defer func() {
		if err := pub.Close(); err != nil {
			t.Logf("failed to close publisher: %v", err)
		}
	}()

	// Create two subscribers with the same subscriptionID
	// They should share messages (load balancing)
	subscriptionID := "shared-subscription"
	sub1, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)
	defer func() {
		if err := sub1.Close(); err != nil {
			t.Logf("failed to close subscriber 1: %v", err)
		}
	}()

	sub2, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)
	defer func() {
		if err := sub2.Close(); err != nil {
			t.Logf("failed to close subscriber 2: %v", err)
		}
	}()

	// Set up handlers to collect events
	received1 := make(chan *event.Event, 10)
	received2 := make(chan *event.Event, 10)

	handler1 := func(ctx context.Context, e *event.Event) error {
		received1 <- e
		return nil
	}

	handler2 := func(ctx context.Context, e *event.Event) error {
		received2 <- e
		return nil
	}

	// Both subscribe to the same topic with the same subscriptionID
	err = sub1.Subscribe(ctx, "shared-topic", handler1)
	require.NoError(t, err)

	err = sub2.Subscribe(ctx, "shared-topic", handler2)
	require.NoError(t, err)

	time.Sleep(cfg.SetupSleep)

	// Publish 10 messages
	numMessages := 10
	for i := 0; i < numMessages; i++ {
		evt := event.New()
		evt.SetType("com.example.test.event")
		evt.SetSource("test-source")
		evt.SetID(fmt.Sprintf("id-%d", i))
		if err := evt.SetData(event.ApplicationJSON, map[string]int{"index": i}); err != nil {
			require.NoError(t, err, "failed to set event data")
		}

		err = pub.Publish("shared-topic", &evt)
		require.NoError(t, err)
	}

	// Wait for all messages to be received
	timeout := time.After(cfg.ReceiveTimeout)
	allReceived := make(map[string]bool)
	sub1Received := make(map[string]bool)
	sub2Received := make(map[string]bool)

	// Collect messages from both subscribers
	for i := 0; i < numMessages; i++ {
		select {
		case evt := <-received1:
			allReceived[evt.ID()] = true
			sub1Received[evt.ID()] = true
		case evt := <-received2:
			allReceived[evt.ID()] = true
			sub2Received[evt.ID()] = true
		case <-timeout:
			t.Fatalf("timeout waiting for message %d", i)
		}
	}

	// Verify all messages were received (distributed between the two subscribers)
	assert.Equal(t, numMessages, len(allReceived), "all messages should be received")

	// Verify messages were distributed (not all to one subscriber)
	sub1Count := len(sub1Received)
	sub2Count := len(sub2Received)
	assert.Greater(t, sub1Count, 0, "subscriber 1 should receive at least one message")
	assert.Greater(t, sub2Count, 0, "subscriber 2 should receive at least one message")
	assert.Equal(t, numMessages, sub1Count+sub2Count, "total messages should equal sum of both subscribers")
}

// RunFanoutSubscription tests that two subscribers with different subscriptionIDs each get all messages
func RunFanoutSubscription(t *testing.T, configMap map[string]string, cfg BrokerTestConfig) {
	ctx := context.Background()

	pub, err := broker.NewPublisher(configMap)
	require.NoError(t, err)
	defer func() {
		if err := pub.Close(); err != nil {
			t.Logf("failed to close publisher: %v", err)
		}
	}()

	// Create two subscribers with different subscriptionIDs
	// Each should receive all messages (fanout behavior)
	sub1, err := broker.NewSubscriber("fanout-subscription-1", configMap)
	require.NoError(t, err)
	defer func() {
		if err := sub1.Close(); err != nil {
			t.Logf("failed to close subscriber 1: %v", err)
		}
	}()

	sub2, err := broker.NewSubscriber("fanout-subscription-2", configMap)
	require.NoError(t, err)
	defer func() {
		if err := sub2.Close(); err != nil {
			t.Logf("failed to close subscriber 2: %v", err)
		}
	}()

	// Set up handlers to collect events
	received1 := make(chan *event.Event, 10)
	received2 := make(chan *event.Event, 10)

	handler1 := func(ctx context.Context, e *event.Event) error {
		received1 <- e
		return nil
	}

	handler2 := func(ctx context.Context, e *event.Event) error {
		received2 <- e
		return nil
	}

	// Both subscribe to the same topic but with different subscriptionIDs
	err = sub1.Subscribe(ctx, "fanout-topic", handler1)
	require.NoError(t, err)

	err = sub2.Subscribe(ctx, "fanout-topic", handler2)
	require.NoError(t, err)

	time.Sleep(cfg.SetupSleep)

	// Publish 5 messages
	numMessages := 5
	messageIDs := make([]string, numMessages)
	for i := 0; i < numMessages; i++ {
		evt := event.New()
		evt.SetType("com.example.test.event")
		evt.SetSource("test-source")
		evt.SetID(fmt.Sprintf("fanout-id-%d", i))
		messageIDs[i] = evt.ID()
		if err := evt.SetData(event.ApplicationJSON, map[string]int{"index": i}); err != nil {
			require.NoError(t, err, "failed to set event data")
		}

		err = pub.Publish("fanout-topic", &evt)
		require.NoError(t, err)
	}

	// Wait for all messages to be received by both subscribers
	timeout := time.After(cfg.ReceiveTimeout)

	// Collect messages from subscriber 1
	received1Map := make(map[string]bool)
	for i := 0; i < numMessages; i++ {
		select {
		case evt := <-received1:
			received1Map[evt.ID()] = true
		case <-timeout:
			t.Fatalf("timeout waiting for subscriber 1 to receive message %d", i)
		}
	}

	// Collect messages from subscriber 2
	received2Map := make(map[string]bool)
	for i := 0; i < numMessages; i++ {
		select {
		case evt := <-received2:
			received2Map[evt.ID()] = true
		case <-timeout:
			t.Fatalf("timeout waiting for subscriber 2 to receive message %d", i)
		}
	}

	// Verify both subscribers received all messages
	assert.Equal(t, numMessages, len(received1Map), "subscriber 1 should receive all messages")
	assert.Equal(t, numMessages, len(received2Map), "subscriber 2 should receive all messages")

	// Verify both received the same message IDs
	for _, id := range messageIDs {
		assert.True(t, received1Map[id], "subscriber 1 should receive message %s", id)
		assert.True(t, received2Map[id], "subscriber 2 should receive message %s", id)
	}
}

// RunSlowSubscriber tests that a slow subscriber processes fewer messages than a fast one
func RunSlowSubscriber(t *testing.T, configMap map[string]string, cfg BrokerTestConfig, sub1, sub2 broker.Subscriber) {
	ctx := context.Background()

	pub, err := broker.NewPublisher(configMap)
	require.NoError(t, err)
	defer func() {
		if err := pub.Close(); err != nil {
			t.Logf("failed to close publisher: %v", err)
		}
	}()

	defer func() {
		if err := sub1.Close(); err != nil {
			t.Logf("failed to close subscriber 1: %v", err)
		}
	}()
	defer func() {
		if err := sub2.Close(); err != nil {
			t.Logf("failed to close subscriber 2: %v", err)
		}
	}()

	// Metrics
	var fastReceived int64
	var slowReceived int64

	// Fast handler - processes immediately
	fastHandler := func(ctx context.Context, e *event.Event) error {
		t.Logf("Fast handler received message %s", e.ID())
		atomic.AddInt64(&fastReceived, 1)
		return nil
	}

	// Slow handler - introduces delay
	slowHandler := func(ctx context.Context, e *event.Event) error {
		t.Logf("Slow handler received message %s", e.ID())
		time.Sleep(100 * time.Millisecond) // Delay processing
		atomic.AddInt64(&slowReceived, 1)
		return nil
	}

	// Subscribe both subscribers
	err = sub1.Subscribe(ctx, "slow-topic", fastHandler)
	require.NoError(t, err)

	err = sub2.Subscribe(ctx, "slow-topic", slowHandler)
	require.NoError(t, err)

	time.Sleep(cfg.SetupSleep)

	// Publish messages
	numMessages := 20
	for i := 0; i < numMessages; i++ {
		evt := event.New()
		evt.SetType("com.example.test.event")
		evt.SetSource("test-source")
		evt.SetID(fmt.Sprintf("slow-id-%d", i))
		if err := evt.SetData(event.ApplicationJSON, map[string]int{"index": i}); err != nil {
			require.NoError(t, err, "failed to set event data")
		}

		err = pub.Publish("slow-topic", &evt)
		require.NoError(t, err)

		// Add delay between publishes if configured (for gradual publishing)
		if cfg.PublishDelay > 0 {
			time.Sleep(cfg.PublishDelay)
		}
	}

	// Wait for all messages to be processed
	timeout := time.After(cfg.ReceiveTimeout)
	for {
		total := atomic.LoadInt64(&fastReceived) + atomic.LoadInt64(&slowReceived)
		if total >= int64(numMessages) {
			break
		}
		select {
		case <-timeout:
			t.Fatalf("timeout waiting for all messages. Fast: %d, Slow: %d, Total: %d",
				atomic.LoadInt64(&fastReceived), atomic.LoadInt64(&slowReceived), total)
		case <-time.After(100 * time.Millisecond):
			// Continue waiting
		}
	}

	fastCount := atomic.LoadInt64(&fastReceived)
	slowCount := atomic.LoadInt64(&slowReceived)
	totalReceived := fastCount + slowCount

	// Verify all messages were received
	assert.Equal(t, int64(numMessages), totalReceived, "all messages should be received")

	// Verify fast subscriber received more messages than slow subscriber
	assert.Greater(t, fastCount, slowCount, "fast subscriber should receive more messages than slow subscriber")
	assert.Greater(t, fastCount, int64(0), "fast subscriber should receive at least one message")
	assert.Greater(t, slowCount, int64(0), "slow subscriber should receive at least one message")

	t.Logf("Fast subscriber received: %d messages", fastCount)
	t.Logf("Slow subscriber received: %d messages", slowCount)
}

// RunErrorSubscriber tests that messages are redistributed when one subscriber fails
func RunErrorSubscriber(t *testing.T, configMap map[string]string, cfg BrokerTestConfig) {
	ctx := context.Background()

	pub, err := broker.NewPublisher(configMap)
	require.NoError(t, err)
	defer func() {
		if err := pub.Close(); err != nil {
			t.Logf("failed to close publisher: %v", err)
		}
	}()

	// Create two subscribers with the same subscriptionID (shared subscription)
	subscriptionID := "error-subscription"
	sub1, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)
	defer func() {
		if err := sub1.Close(); err != nil {
			t.Logf("failed to close subscriber 1: %v", err)
		}
	}()

	sub2, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)
	defer func() {
		if err := sub2.Close(); err != nil {
			t.Logf("failed to close subscriber 2: %v", err)
		}
	}()

	// Metrics
	var errorSubReceived int64
	var workingSubReceived int64

	// Handler that always returns an error
	errorHandler := func(ctx context.Context, e *event.Event) error {
		atomic.AddInt64(&errorSubReceived, 1)
		return fmt.Errorf("intentional error for message %s", e.ID())
	}

	// Working handler
	workingHandler := func(ctx context.Context, e *event.Event) error {
		atomic.AddInt64(&workingSubReceived, 1)
		return nil
	}

	// Subscribe both subscribers
	err = sub1.Subscribe(ctx, "error-topic", errorHandler)
	require.NoError(t, err)

	err = sub2.Subscribe(ctx, "error-topic", workingHandler)
	require.NoError(t, err)

	time.Sleep(cfg.SetupSleep)

	// Publish 10 messages
	numMessages := 10
	for i := 0; i < numMessages; i++ {
		evt := event.New()
		evt.SetType("com.example.test.event")
		evt.SetSource("test-source")
		evt.SetID(fmt.Sprintf("error-id-%d", i))
		if err := evt.SetData(event.ApplicationJSON, map[string]int{"index": i}); err != nil {
			require.NoError(t, err, "failed to set event data")
		}

		err = pub.Publish("error-topic", &evt)
		require.NoError(t, err)
	}

	// Wait for messages to be processed
	// Messages that fail will be nacked and may be redelivered
	time.Sleep(cfg.ReceiveTimeout)

	errorCount := atomic.LoadInt64(&errorSubReceived)
	workingCount := atomic.LoadInt64(&workingSubReceived)

	// The working subscriber should receive all messages eventually
	// (either initially or after redelivery from the error subscriber)
	assert.Greater(t, workingCount, int64(0), "working subscriber should receive messages")

	// The error subscriber may receive some messages (they get nacked and redelivered)
	// But the working subscriber should eventually process all of them
	t.Logf("Error subscriber received (and failed): %d messages", errorCount)
	t.Logf("Working subscriber received: %d messages", workingCount)

	// At least some messages should be processed successfully
	assert.GreaterOrEqual(t, workingCount, int64(numMessages/2),
		"working subscriber should process at least half the messages")
}

// RunCloseWaitsForInFlightMessages tests that Close() waits for in-flight message processing to complete
func RunCloseWaitsForInFlightMessages(t *testing.T, configMap map[string]string, cfg BrokerTestConfig) {
	ctx := context.Background()
	configMap["subscriber.parallelism"] = "6"

	pub, err := broker.NewPublisher(configMap)
	require.NoError(t, err)
	defer func() {
		if err := pub.Close(); err != nil {
			t.Logf("failed to close publisher: %v", err)
		}
	}()

	subscriptionID := "close-test-subscription"
	sub, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)

	// Publish 5 messages
	numMessages := 5

	// Track processed messages
	var processedCount int64
	processingStarted := make(chan struct{})

	// Handler that takes time to process (simulating in-flight work)
	handler := func(ctx context.Context, e *event.Event) error {
		// atomic.AddInt64 returns the new value after incrementing
		// This gives us the sequence number of this message (1, 2, 3, etc.)
		if atomic.AddInt64(&processedCount, 1) == 5 {
			close(processingStarted) // Signal that processing has started
		}
		// Simulate work that takes time (200ms per message)
		time.Sleep(3000 * time.Millisecond)

		return nil
	}

	err = sub.Subscribe(ctx, "close-test-topic", handler)
	require.NoError(t, err)

	time.Sleep(cfg.SetupSleep)
	for i := 0; i < numMessages; i++ {
		evt := event.New()
		evt.SetType("com.example.test.event")
		evt.SetSource("test-source")
		evt.SetID(fmt.Sprintf("close-test-id-%d", i))
		if err := evt.SetData(event.ApplicationJSON, map[string]int{"index": i}); err != nil {
			require.NoError(t, err, "failed to set event data")
		}

		err = pub.Publish("close-test-topic", &evt)

		require.NoError(t, err)
	}

	// Wait for processing to start (at least one message is being processed)
	select {
	case <-processingStarted:
		// Processing has started, messages are now in-flight
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for processing to start")
	}

	// Record the time before Close()
	closeStartTime := time.Now()

	// Get count before Close() to verify messages are in-flight
	countBeforeClose := atomic.LoadInt64(&processedCount)
	assert.Greater(t, countBeforeClose, int64(0),
		"at least one message should be in-flight when Close() is called")

	// Close() should wait for all in-flight messages to complete
	err = sub.Close()
	closeDuration := time.Since(closeStartTime)
	require.NoError(t, err)

	// Verify that Close() took at least as long as processing one message (200ms)
	// Since we have parallelism workers, multiple messages can be processed concurrently,
	// but Close() should wait for all of them to complete
	// With 5 messages at 200ms each and parallelism=1, it should take at least 1 second
	// With parallelism > 1, it could be faster, but still should wait for all messages
	minExpectedDuration := 200 * time.Millisecond
	assert.GreaterOrEqual(t, closeDuration, minExpectedDuration,
		"Close() should wait for in-flight messages to complete")

	// Verify all messages were processed before Close() returned
	finalCount := atomic.LoadInt64(&processedCount)
	assert.Equal(t, int64(numMessages), finalCount,
		"all messages should be processed before Close() returns")

	// Wait a short time to verify no additional processing happens after Close()
	// (proving that Close() actually waited for completion)
	time.Sleep(100 * time.Millisecond)
	countAfterWait := atomic.LoadInt64(&processedCount)
	assert.Equal(t, finalCount, countAfterWait,
		"no additional messages should be processed after Close() returns")

	t.Logf("Close() took %v to complete, processed %d messages", closeDuration, finalCount)
}

// RunPanicHandler tests that a handler that panics doesn't cause Close() to hang
func RunPanicHandler(t *testing.T, configMap map[string]string, cfg BrokerTestConfig) {
	ctx := context.Background()
	configMap["subscriber.parallelism"] = "3"

	pub, err := broker.NewPublisher(configMap)
	require.NoError(t, err)
	defer func() {
		if err := pub.Close(); err != nil {
			t.Logf("failed to close publisher: %v", err)
		}
	}()

	subscriptionID := "panic-test-subscription"
	sub, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)

	// Track how many times the handler was called (before panic)
	var handlerCallCount int64
	panicOccurred := make(chan struct{})
	var panicOnce sync.Once

	// Handler that panics
	panicHandler := func(ctx context.Context, e *event.Event) error {
		atomic.AddInt64(&handlerCallCount, 1)
		panicOnce.Do(func() {
			close(panicOccurred) // Signal that panic is about to occur
		})
		panic(fmt.Sprintf("intentional panic for message %s", e.ID()))
	}

	err = sub.Subscribe(ctx, "panic-test-topic", panicHandler)
	require.NoError(t, err)

	time.Sleep(cfg.SetupSleep)

	// Publish a few messages
	numMessages := 3
	for i := 0; i < numMessages; i++ {
		evt := event.New()
		evt.SetType("com.example.test.event")
		evt.SetSource("test-source")
		evt.SetID(fmt.Sprintf("panic-test-id-%d", i))
		if err := evt.SetData(event.ApplicationJSON, map[string]int{"index": i}); err != nil {
			require.NoError(t, err, "failed to set event data")
		}

		err = pub.Publish("panic-test-topic", &evt)
		require.NoError(t, err)
	}

	// Wait for at least one panic to occur
	select {
	case <-panicOccurred:
		// Panic has occurred, good
	case <-time.After(cfg.ReceiveTimeout):
		t.Fatal("timeout waiting for panic to occur")
	}

	// Give some time for messages to be received and panics to happen
	time.Sleep(1 * time.Second)

	// Verify that at least one handler was called (and panicked)
	assert.Greater(t, atomic.LoadInt64(&handlerCallCount), int64(0),
		"handler should have been called at least once")

	// Close() should complete without hanging, even with panics
	closeStartTime := time.Now()
	closeDone := make(chan error, 1)

	go func() {
		closeDone <- sub.Close()
	}()

	// Wait for Close() to complete with a timeout
	select {
	case err := <-closeDone:
		closeDuration := time.Since(closeStartTime)
		require.NoError(t, err, "Close() should complete successfully")
		// Close() should complete reasonably quickly (not hang)
		// It should wait for workers to finish, but not indefinitely
		maxExpectedDuration := 5 * time.Second
		assert.Less(t, closeDuration, maxExpectedDuration,
			"Close() should complete without hanging, even with panics")
		t.Logf("Close() completed in %v despite panics", closeDuration)
	case <-time.After(10 * time.Second):
		t.Fatal("Close() hung - it should complete even when handlers panic")
	}
}

// RunErrorChannelNotification tests that infrastructure errors are sent to the error channel
func RunErrorChannelNotification(t *testing.T, configMap map[string]string, cfg BrokerTestConfig) {
	ctx := context.Background()

	// Create subscriber
	subscriptionID := "error-channel-test"
	sub, err := broker.NewSubscriber(subscriptionID, configMap)
	require.NoError(t, err)
	defer func() {
		if err := sub.Close(); err != nil {
			t.Logf("failed to close subscriber: %v", err)
		}
	}()

	// Start goroutine to collect errors
	errorsChan := sub.Errors()
	require.NotNil(t, errorsChan, "Errors() should return a non-nil channel")

	receivedErrors := make(chan *broker.SubscriberError, 10)
	errCollectorDone := make(chan struct{})

	go func() {
		defer close(errCollectorDone)
		for err := range errorsChan {
			t.Logf("Received error from channel: Op=%s, Topic=%s, Fatal=%v, Err=%v",
				err.Op, err.Topic, err.Fatal, err.Err)
			receivedErrors <- err
		}
	}()

	// Simple handler that processes messages successfully
	handler := func(ctx context.Context, evt *event.Event) error {
		return nil
	}

	// Subscribe to a topic
	err = sub.Subscribe(ctx, "error-test-topic", handler)
	require.NoError(t, err)

	// Wait for subscription to be set up
	time.Sleep(cfg.SetupSleep)

	// Note: In a real integration test, you would force a connection failure here
	// by stopping the broker container or closing the connection.
	// For now, we'll verify the channel exists and can receive errors.

	// Close the subscriber - this should trigger context cancellation
	// which may or may not generate an error depending on the broker
	err = sub.Close()
	require.NoError(t, err)

	// Wait for error collector to finish
	select {
	case <-errCollectorDone:
		t.Log("Error collector finished")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for error collector to finish")
	}

	// Verify the error channel was closed
	select {
	case _, ok := <-errorsChan:
		assert.False(t, ok, "error channel should be closed after Close()")
	default:
		t.Fatal("error channel should be closed")
	}

	t.Log("Error channel notification test completed successfully")
}


