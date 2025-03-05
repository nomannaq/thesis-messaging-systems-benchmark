package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/streadway/amqp"
)

type Result struct {
	latency time.Duration
	size    int // Size of the message in bytes
}

func main() {
	var batchSize int
	fmt.Print("Enter batch size: ")
	fmt.Scan(&batchSize)

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

	msgs, err := ch.Consume(
		queueName, // queue
		"",        // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	resultChan := make(chan Result, 10000)
	var wg sync.WaitGroup
	workers := 16 // Increase based on CPU cores

	// Start workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for res := range resultChan {
				// Simulate processing time
				_ = res // Process result here
			}
		}()
	}

	var (
		totalMsg     int
		totalLatency time.Duration
		totalBytes   int64 // Track total bytes processed
		minLatency   = time.Hour
		maxLatency   time.Duration
		startTime    time.Time // Start time for processing
	)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("Consumer started (workers=%d)\n", workers)

BatchLoop:
	for {
		select {
		case sig := <-sigchan:
			fmt.Printf("Terminating: %v\n", sig)
			break BatchLoop
		default:
			select {
			case msg := <-msgs:
				// Start the timer when the first message is received
				if totalMsg == 0 {
					startTime = time.Now()
				}

				latency := time.Since(msg.Timestamp)
				resultChan <- Result{latency: latency, size: len(msg.Body)}

				totalMsg++
				totalLatency += latency
				totalBytes += int64(len(msg.Body)) // Accumulate total bytes
				if latency < minLatency {
					minLatency = latency
				}
				if latency > maxLatency {
					maxLatency = latency
				}

				if totalMsg >= batchSize {
					break BatchLoop
				}
			case <-time.After(100 * time.Millisecond):
				// Timeout to mimic Kafka's polling behavior
			}
		}
	}

	close(resultChan)
	wg.Wait()
	elapsed := time.Since(startTime)
	fmt.Printf("\n=== Consumer Metrics ===\n")
	fmt.Printf("Messages received: %d\n", totalMsg)
	fmt.Printf("Time elapsed: %.2f seconds\n", elapsed.Seconds())
	fmt.Printf("Throughput: %.2f messages/second\n", float64(totalMsg)/elapsed.Seconds())
	fmt.Printf("Throughput: %.2f MB/second\n", float64(totalBytes)/1024/1024/elapsed.Seconds())

	// Calculate average latency
	avgLatency := time.Duration(int64(totalLatency) / int64(totalMsg))

	fmt.Printf("Latency (min/mean/max): %v / %v / %v\n",
		minLatency.Round(time.Microsecond),
		avgLatency.Round(time.Microsecond),
		maxLatency.Round(time.Microsecond))
}
