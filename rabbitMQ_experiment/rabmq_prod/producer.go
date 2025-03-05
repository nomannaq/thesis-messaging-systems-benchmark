package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
	"golang.org/x/time/rate"
)

func main() {
	var messageSize, totalMessages int64
	var messageRate int

	fmt.Print("Enter message size (bytes): ")
	fmt.Scan(&messageSize)
	fmt.Print("Enter message rate (messages/sec): ")
	fmt.Scan(&messageRate)
	fmt.Print("Enter total messages to send: ")
	fmt.Scan(&totalMessages)

	rabbitMQURL := "amqp://guest:guest@localhost:5672/"
	queueName := "test_queue"

	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	payload := make([]byte, messageSize)
	for i := range payload {
		payload[i] = 'A'
	}

	var (
		wg           sync.WaitGroup
		successCount atomic.Int64
		failureCount atomic.Int64
		limiter      = rate.NewLimiter(rate.Limit(messageRate), messageRate)
		workers      = 16
		messagesLeft = totalMessages
		start        = time.Now()
	)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for atomic.LoadInt64(&messagesLeft) > 0 {
				if err := limiter.WaitN(context.Background(), 1); err != nil {
					log.Printf("Rate limiter error: %v", err)
					continue
				}

				if atomic.AddInt64(&messagesLeft, -1) < 0 {
					return
				}

				err := ch.Publish(
					"",        // exchange
					queueName, // routing key
					false,     // mandatory
					false,     // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        payload,
						Timestamp:   time.Now().UTC(),
					},
				)

				if err != nil {
					failureCount.Add(1)
				} else {
					successCount.Add(1)
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("\n=== Producer Metrics ===\n")
	fmt.Printf("Attempted: %d\n", totalMessages)
	fmt.Printf("Successful: %d\n", successCount.Load())
	fmt.Printf("Failed: %d\n", failureCount.Load())
	fmt.Printf("Throughput: %.2f msg/s\n", float64(successCount.Load())/elapsed.Seconds())
	fmt.Printf("Throughput: %.2f MB/s\n",
		(float64(successCount.Load()*messageSize) / 1024 / 1024 / elapsed.Seconds()))
}
