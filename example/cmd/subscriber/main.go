package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/openshift-hyperfleet/hyperfleet-broker/broker"
)

func main() {
	processTime := flag.Duration("process-time", 2*time.Second, "Time to simulate message processing")
	topic := flag.String("topic", "example-topic", "Topic to subscribe to")
	subscription := flag.String("subscription", "shared-subscription", "Subscription ID for load balancing")
	flag.Parse()

	// Get subscriber instance ID from environment variable (defaults to "1")
	instanceID := os.Getenv("SUBSCRIBER_INSTANCE_ID")
	if instanceID == "" {
		instanceID = "1"
	}

	// Create subscriber with subscription ID
	// Both subscribers use the same subscription ID to share messages (load balancing)
	subscriber, err := broker.NewSubscriber(*subscription)
	if err != nil {
		log.Fatalf("Failed to create subscriber: %v", err)
	}
	defer subscriber.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("Subscriber instance %s started. Listening on topic: %s with subscription ID: %s", instanceID, *topic, *subscription)

	// Start error handler goroutine to monitor infrastructure errors
	go func() {
		for err := range subscriber.Errors() {
			log.Printf("[Subscriber %s] ERROR: Op=%s, Topic=%s, Fatal=%v, Error=%v",
				instanceID, err.Op, err.Topic, err.Fatal, err.Err)

			if err.Fatal {
				log.Printf("[Subscriber %s] FATAL ERROR: Subscriber has stopped. Initiating shutdown...", instanceID)
				// Trigger graceful shutdown on fatal errors
				cancel()
			}
		}
		log.Printf("[Subscriber %s] Error channel closed", instanceID)
	}()

	// Channel to signal shutdown from message handler
	shutdownChan := make(chan struct{}, 1)

	// Define handler
	handler := func(ctx context.Context, evt *event.Event) error {
		log.Printf("[Subscriber %s] Received event - ID: %s, Type: %s, Source: %s",
			instanceID, evt.ID(), evt.Type(), evt.Source())

		// Extract data
		var data map[string]interface{}
		if err := evt.DataAs(&data); err == nil {
			log.Printf("[Subscriber %s] Event data: %+v", instanceID, data)

			// Check if message contains "close" command
			if closeCmd, ok := data["close"]; ok {
				if closeCmd == "true" || closeCmd == true {
					log.Printf("[Subscriber %s] Received close command. Initiating graceful shutdown...", instanceID)
					select {
					case shutdownChan <- struct{}{}:
					default:
						// Already signaled
					}
					return nil
				}
			}
		}

		time.Sleep(*processTime)
		log.Printf("[Subscriber %s] Processed event - ID: %s, Type: %s, Source: %s",
			instanceID, evt.ID(), evt.Type(), evt.Source())
		return nil
	}

	// Subscribe to topic
	if err := subscriber.Subscribe(ctx, *topic, handler); err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// Wait for interrupt signal or shutdown command
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	select {
	case <-sigChan:
		log.Printf("[Subscriber %s] Received interrupt signal", instanceID)
	case <-shutdownChan:
		log.Printf("[Subscriber %s] Received shutdown command from message", instanceID)
	case <-ctx.Done():
		log.Printf("[Subscriber %s] Context cancelled (fatal error occurred)", instanceID)
	}

	log.Printf("Shutting down subscriber instance %s...", instanceID)
}
