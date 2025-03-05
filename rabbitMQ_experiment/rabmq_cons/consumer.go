package main

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func main() {
	// RabbitMQ connection settings
	rabbitMQURL := "amqp://guest:guest@localhost:5672/"
	queueName := "test_queue"

	// Connect to RabbitMQ
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %s", err)
	}
	defer conn.Close()

	// Create a channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %s", err)
	}
	defer ch.Close()

	// Declare the queue (must match producer settings)
	_, err = ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %s", err)
	}

	// Consume messages
	msgs, err := ch.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack (manual acknowledgment)
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		log.Fatalf("Failed to consume messages: %s", err)
	}

	var latencies []float64
	var minLatency, maxLatency float64

	fmt.Println("Consumer started. Waiting for messages...")

	for msg := range msgs {
		// Extract timestamp from headers
		sendTime := msg.Headers["timestamp"].(int64)
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
		fmt.Printf("Received message: %s\n", msg.Body)

		// Acknowledge the message
		msg.Ack(false)

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
