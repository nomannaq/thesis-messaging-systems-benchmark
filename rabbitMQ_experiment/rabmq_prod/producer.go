package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/streadway/amqp"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("Usage: go run producer.go <messageSize> <duration> (e.g., 1024 1m)")
	}
	messageSize, err := strconv.ParseInt(os.Args[1], 10, 64)
	if err != nil {
		log.Fatalf("Invalid message size: %v", err)
	}
	runDuration, err := time.ParseDuration(os.Args[2])
	if err != nil {
		log.Fatalf("Invalid duration format: %v", err)
	}

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
		queueName, false, false, false, false, nil,
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
		workers      = runtime.NumCPU() * 2
		shutdown     = make(chan struct{})
		start        = time.Now()
		endTime      = start.Add(runDuration)
	)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigchan
		fmt.Println("Received termination signal. Initiating shutdown...")
		close(shutdown)
	}()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-shutdown:
					return
				default:
					if time.Now().After(endTime) {
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
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("\n=== Producer Metrics ===\n")
	fmt.Printf("Run Duration: %s\n", runDuration)
	fmt.Printf("Elapsed: %s\n", elapsed)
	fmt.Printf("Successful: %d\n", successCount.Load())
	fmt.Printf("Failed: %d\n", failureCount.Load())
	fmt.Printf("Throughput: %.2f msg/s\n", float64(successCount.Load())/elapsed.Seconds())
	fmt.Printf("Throughput: %.2f MB/s\n",
		(float64(successCount.Load()*messageSize) / 1024 / 1024 / elapsed.Seconds()))
	fmt.Println("Producer closed gracefully.")
}
