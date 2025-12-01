package broker

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/cloudevents/sdk-go/v2/event"
)

const (
	DefaultSubscriberParallelism = 1
	DefaultTestParallelism       = 1
	DefaultShutdownTimeout       = 30 * time.Second
)

// NewPublisher creates a new publisher based on the configuration.
// If configMap is provided, it will be used instead of loading from file.
// Providing a configMap is meant to be used for testing purposes only.
// Config keys should use dot notation (e.g., "broker.type", "broker.rabbitmq.url").
func NewPublisher(configMap ...map[string]string) (Publisher, error) {
	var cfg *config

	if len(configMap) > 0 && configMap[0] != nil {
		var err error
		cfg, err = buildConfigFromMap(configMap[0])
		if err != nil {
			return nil, fmt.Errorf("failed to build config from map: %w", err)
		}
	} else {
		var err error
		cfg, err = loadConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to load config: %w", err)
		}
	}

	logger := watermill.NewStdLogger(false, false)

	// Log configuration before creating publisher if enabled
	if cfg.LogConfig {
		logConfiguration(cfg, "Publisher", logger)
	}

	var pub message.Publisher
	var err error

	switch cfg.Broker.Type {
	case "rabbitmq":
		pub, err = newRabbitMQPublisher(cfg, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create RabbitMQ publisher: %w", err)
		}
	case "googlepubsub":
		pub, err = newGooglePubSubPublisher(cfg, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create Google Pub/Sub publisher: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported broker type: %s", cfg.Broker.Type)
	}

	return &publisher{pub: pub}, nil
}

// NewSubscriber creates a new subscriber based on the configuration.
// subscriptionID determines whether subscribers share messages (same ID = shared, different IDs = separate).
// If configMap is provided, it will be used instead of loading from file.
// Config keys should use dot notation (e.g., "broker.type", "broker.rabbitmq.url").
func NewSubscriber(subscriptionID string, configMap ...map[string]string) (Subscriber, error) {
	if subscriptionID == "" {
		return nil, fmt.Errorf("subscriptionID is required")
	}

	var cfg *config

	if len(configMap) > 0 && configMap[0] != nil {
		var err error
		cfg, err = buildConfigFromMap(configMap[0])
		if err != nil {
			return nil, fmt.Errorf("failed to build config from map: %w", err)
		}
	} else {
		var err error
		cfg, err = loadConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to load config: %w", err)
		}
	}

	logger := watermill.NewStdLogger(false, false)

	// Log configuration before creating subscriber if enabled
	if cfg.LogConfig {
		logConfiguration(cfg, "Subscriber", logger)
	}

	var sub message.Subscriber
	var err error

	switch cfg.Broker.Type {
	case "rabbitmq":
		sub, err = newRabbitMQSubscriber(cfg, logger, subscriptionID)
		if err != nil {
			return nil, fmt.Errorf("failed to create RabbitMQ subscriber: %w", err)
		}
	case "googlepubsub":
		sub, err = newGooglePubSubSubscriber(cfg, logger, subscriptionID)
		if err != nil {
			return nil, fmt.Errorf("failed to create Google Pub/Sub subscriber: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported broker type: %s", cfg.Broker.Type)
	}

	parallelism := cfg.Subscriber.Parallelism
	if parallelism <= 0 {
		parallelism = DefaultSubscriberParallelism
	}
	return &subscriber{
		sub:            sub,
		parallelism:    parallelism,
		subscriptionID: subscriptionID,
		logger:         logger,
		errorChan:      make(chan *SubscriberError, ErrorChannelBufferSize),
	}, nil
}

// logConfiguration logs the complete configuration object as JSON
func logConfiguration(cfg *config, component string, logger watermill.LoggerAdapter) {
	// Create a copy of config with masked password for logging
	logCfg := *cfg
	if cfg.Broker.Type == "rabbitmq" && cfg.Broker.RabbitMQ.URL != "" {
		logCfg.Broker.RabbitMQ.URL = maskPassword(cfg.Broker.RabbitMQ.URL)
	}

	// Marshal to JSON with indentation
	jsonBytes, err := json.MarshalIndent(logCfg, "", "  ")
	if err != nil {
		logger.Error(fmt.Sprintf("Error marshaling %s configuration to JSON", component), err, nil)
		// Fallback to simple logging
		logger.Info(fmt.Sprintf("=== %s Configuration ===", component), nil)
		logger.Info(fmt.Sprintf("Broker Type: %s", cfg.Broker.Type), nil)
		return
	}

	logger.Info(fmt.Sprintf("=== %s Configuration (JSON) ===\n%s\n========================================", component, string(jsonBytes)), nil)
}

// maskPassword masks passwords in URLs for logging
func maskPassword(url string) string {
	if url == "" {
		return ""
	}
	// Look for password pattern in URL (e.g., amqp://user:pass@host)
	if idx := strings.Index(url, "@"); idx > 0 {
		// Find the last colon before @
		if colonIdx := strings.LastIndex(url[:idx], ":"); colonIdx > 0 {
			// Mask the password part
			return url[:colonIdx+1] + "***" + url[idx:]
		}
	}
	return url
}

// messageToEvent converts a Watermill message to a CloudEvent
func messageToEvent(msg *message.Message) (*event.Event, error) {
	evt := event.New()

	// Unmarshal the event from the payload (Structured Content Mode)
	if err := json.Unmarshal(msg.Payload, &evt); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event from JSON: %w", err)
	}

	return &evt, nil
}

// eventToMessage converts a CloudEvent to a Watermill message
func eventToMessage(evt *event.Event) (*message.Message, error) {
	// Marshal the event to JSON (Structured Content Mode)
	payload, err := json.Marshal(evt)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event to JSON: %w", err)
	}

	msg := message.NewMessage(evt.ID(), payload)
	return msg, nil
}
