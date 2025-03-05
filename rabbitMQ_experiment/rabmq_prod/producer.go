package main

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
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

	// Declare a durable queue
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

	// Generate payload
	payload := make([]byte, messageSize)
	for i := range payload {
		payload[i] = 'A'
	}

	// Start timer
	startTime := time.Now()

	// Publish messages
	for i := 1; i <= totalMessages; i++ {
		err = ch.Publish(
			"",        // exchange (default)
			queueName, // routing key
			false,     // mandatory
			false,     // immediate
			amqp.Publishing{
				DeliveryMode: amqp.Persistent, // Persistent message
				ContentType:  "text/plain",
				Body:         payload,
				Headers: amqp.Table{
					"timestamp": time.Now().UnixNano(), // Add timestamp for latency calculation
				},
			},
		)
		if err != nil {
			log.Fatalf("Failed to publish message: %s", err)
		}

		// Log every sent message
		fmt.Printf("Sent message %d\n", i)

		// Rate control
		if i%messageRate == 0 {
			time.Sleep(time.Second)
		}
	}

	// Calculate throughput
	elapsed := time.Since(startTime)
	throughput := float64(totalMessages) / elapsed.Seconds()
	fmt.Printf("Throughput: %.2f msg/s\n", throughput)
	fmt.Println("Message sending complete. Exiting...")
}
