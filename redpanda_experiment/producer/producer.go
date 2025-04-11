package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"golang.org/x/time/rate"
)

func main() {
	var messageSize int64
	var messageRate int
	var runDuration time.Duration

	fmt.Print("Enter message size (bytes): ")
	fmt.Scan(&messageSize)
	fmt.Print("Enter message rate (messages/sec): ")
	fmt.Scan(&messageRate)
	fmt.Print("Enter run duration (e.g., 5m for 5 minutes): ")
	var durationInput string
	fmt.Scan(&durationInput)

	// Parse the duration input (e.g., "5m" for 5 minutes)
	runDuration, err := time.ParseDuration(durationInput)
	if err != nil {
		log.Fatalf("Invalid duration format: %v", err)
	}

	kafkaBroker := "localhost:9092"
	topic := "test_topic"

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBroker,
		"linger.ms":         0,     // Small batching- no artificial delays
		"batch.size":        64000, // 16KB batch size
		"compression.type":  "lz4",
		"acks":              "all", // Ensure reliability
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	payload := make([]byte, messageSize)
	for i := range payload {
		payload[i] = 'A'
	}

	var (
		wg           sync.WaitGroup
		deliveryChan = make(chan kafka.Event, 10000)
		successCount atomic.Int64
		failureCount atomic.Int64
		limiter      = rate.NewLimiter(rate.Limit(messageRate), messageRate) // Use int for rate
		workers      = runtime.NumCPU() * 2
		shutdown     = make(chan struct{})
		start        = time.Now()
		endTime      = start.Add(runDuration) // Calculate end time
	)

	// Start delivery report handler
	go func() {
		for e := range deliveryChan {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					failureCount.Add(1)
				} else {
					successCount.Add(1)
				}
			}
		}
		fmt.Println("Delivery report handler exiting.")
	}()

	// Signal handling for graceful shutdown
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
					fmt.Println("Worker received shutdown signal.")
					return
				default:
					if time.Now().After(endTime) {
						fmt.Println("Run duration reached. Stopping worker.")
						return
					}

					if err := limiter.WaitN(context.Background(), 1); err != nil {
						log.Printf("Rate limiter error: %v", err)
						continue
					}

					err := producer.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
						Value:          payload,
						Timestamp:      time.Now().UTC(), // Use UTC for consistency
					}, deliveryChan)

					if err != nil {
						failureCount.Add(1)
					}
				}
			}
		}()
	}

	wg.Wait()
	fmt.Println("All workers finished. Flushing remaining messages...")

	// Drain the delivery channel before flushing
	go func() {
		for range deliveryChan {
			// Drain the channel
		}
	}()

	// Flush remaining messages
	producer.Flush(30 * 1000)
	close(deliveryChan)
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
	os.Exit(0)
}
