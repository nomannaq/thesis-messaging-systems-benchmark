package main

import (
	"fmt"
	"log"
	"strconv"
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

	kafkaBroker := "localhost:29092" // Match the broker address from your Kafka setup
	topic := "test_topic"

	// Configure Kafka producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBroker,
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s\n", err)
	}
	defer producer.Close()

	// Generate payload of specified size
	payload := make([]byte, messageSize)
	for i := range payload {
		payload[i] = 'A' // Filling the payload with 'A'
	}

	// Start sending messages
	startTime := time.Now()

	for i := 1; i <= totalMessages; i++ {
		// Send message asynchronously
		err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(strconv.Itoa(i)),
			Value: payload,
		}, nil)

		if err != nil {
			log.Fatalf("Failed to produce message: %s\n", err)
		}

		// Log every sent message
		fmt.Printf("Sent message %d\n", i)

		// Rate control
		if i%messageRate == 0 {
			time.Sleep(time.Second) // Sleep to control the rate (messages/sec)
		}
	}

	const flushTimeout = 10 * 1000 // 10 seconds in milliseconds
	producer.Flush(flushTimeout)   // Flush messages to Kafka
	// Calculate throughput
	elapsed := time.Since(startTime)
	throughput := float64(totalMessages) / elapsed.Seconds()
	fmt.Printf("Throughput: %.2f msg/s\n", throughput)
	fmt.Println("Message sending complete. Exiting...")
}
