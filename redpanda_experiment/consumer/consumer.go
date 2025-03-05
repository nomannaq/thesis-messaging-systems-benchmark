package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// Kafka (Redpanda) configuration
	broker := "localhost:9092"
	topic := "test_topic"
	groupID := "test_group"

	// Create a new consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer consumer.Close()

	// Subscribe to the topic
	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %s", err)
	}

	var latencies []float64
	var minLatency, maxLatency float64

	fmt.Println("Consumer started. Waiting for messages...")

	// Consume messages
	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			log.Fatalf("Failed to read message: %s", err)
		}

		// Extract timestamp from headers
		var sendTime int64
		for _, header := range msg.Headers {
			if header.Key == "timestamp" {
				sendTime, _ = strconv.ParseInt(string(header.Value), 10, 64)
				break
			}
		}
		receiveTime := time.Now().UnixNano()
		latency := float64(receiveTime-sendTime) / 1e6 // Convert ns to ms

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

		// Print the message body
		fmt.Printf("Received message: %s\n", string(msg.Value))

		// Log every 1000 messages
		if len(latencies)%1000 == 0 {
			meanLatency := mean(latencies)
			fmt.Printf(
				"Processed %d messages. Mean latency: %.2f ms, Min latency: %.2f ms, Max latency: %.2f ms\n",
				len(latencies), meanLatency, minLatency, maxLatency,
			)
		}
	}
}

// Helper function to calculate mean latency
func mean(arr []float64) float64 {
	sum := 0.0
	for _, v := range arr {
		sum += v
	}
	return sum / float64(len(arr))
}
