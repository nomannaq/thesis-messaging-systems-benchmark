package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	var messageSize, messageRate, totalMessages int

	// Get user input
	fmt.Print("Enter message size (bytes): ")
	fmt.Scan(&messageSize)
	fmt.Print("Enter message rate (messages/sec): ")
	fmt.Scan(&messageRate)
	fmt.Print("Enter total messages to send: ")
	fmt.Scan(&totalMessages)

	// Kafka (Redpanda) configuration
	broker := "localhost:9092"
	topic := "test_topic"

	// Create a new producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	// Generate payload
	payload := make([]byte, messageSize)
	for i := range payload {
		payload[i] = 'A'
	}

	// Start timer
	startTime := time.Now()

	// Publish messages
	for i := 1; i <= totalMessages; i++ {
		// Create a message with a timestamp header
		message := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          payload,
			Headers: []kafka.Header{
				{Key: "timestamp", Value: []byte(fmt.Sprintf("%d", time.Now().UnixNano()))},
			},
		}

		// Send the message
		err := producer.Produce(message, nil)
		if err != nil {
			log.Fatalf("Failed to produce message: %s", err)
		}

		// Log every sent message
		fmt.Printf("Sent message %d\n", i)

		// Rate control
		if i%messageRate == 0 {
			time.Sleep(time.Second)
		}
	}

	// Wait for all messages to be delivered
	producer.Flush(15 * 1000)

	// Calculate throughput
	elapsed := time.Since(startTime)
	throughput := float64(totalMessages) / elapsed.Seconds()
	fmt.Printf("Throughput: %.2f msg/s\n", throughput)
	fmt.Println("Message sending complete. Exiting...")
}
