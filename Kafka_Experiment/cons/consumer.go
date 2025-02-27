package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	kafkaBroker := "localhost:29092" // Match the broker address from your Kafka setup
	topic := "test_topic"
	groupID := "test_group"

	// Configure Kafka consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBroker,
		"group.id":          groupID,
		"auto.offset.reset": "earliest", // Start reading from the earliest available message
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s\n", err)
	}
	defer consumer.Close()

	// Subscribe to the topic
	err = consumer.Subscribe(topic, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %s\n", err)
	}

	var latencies []float64
	var minLatency, maxLatency float64

	fmt.Println("Consumer started. Waiting for messages...")

	// Consume messages
	for {
		msg, err := consumer.ReadMessage(-1) // Infinite blocking call
		if err != nil {
			log.Printf("Error reading message: %v\n", err)
			continue
		}

		// Calculate latency (time difference between send and receive)
		receiveTime := time.Now()
		sendTime := msg.Timestamp
		latency := receiveTime.Sub(sendTime).Seconds() * 1000 // Convert to ms

		// Append latency to the array
		latencies = append(latencies, latency)

		// Update min and max latency
		if len(latencies) == 1 {
			minLatency, maxLatency = latency, latency
		} else {
			if latency < minLatency {
				minLatency = latency
			}
			if latency > maxLatency {
				maxLatency = latency
			}
		}

		// Print message details
		fmt.Printf(
			"Received message: Key=%s, Partition=%d, Offset=%d, Timestamp=%s, Latency=%.2fms\n",
			string(msg.Key), msg.TopicPartition.Partition, msg.TopicPartition.Offset, msg.Timestamp.Format(time.RFC3339), latency,
		)

		// Log statistics every 1000 messages
		if len(latencies)%1000 == 0 {
			meanLatency := mean(latencies)
			fmt.Printf("Processed %d messages. Mean latency: %.2f ms, Min latency: %.2f ms, Max latency: %.2f ms\n",
				len(latencies), meanLatency, minLatency, maxLatency)
		}
	}
}

// Helper function to calculate the mean of an array of latencies
func mean(arr []float64) float64 {
	sum := 0.0
	for _, v := range arr {
		sum += v
	}
	return sum / float64(len(arr))
}
