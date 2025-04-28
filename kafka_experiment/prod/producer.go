package main

import (
	"context"
	"flag"
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

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"golang.org/x/time/rate"
)

func main() {
	var messageSize int64
	var messageRate int
	var runDuration time.Duration

	// Parse command-line flags
	flag.Int64Var(&messageSize, "size", 0, "Message size in bytes")
	flag.IntVar(&messageRate, "rate", 0, "Message rate in messages/sec")
	flag.DurationVar(&runDuration, "duration", 0, "Run duration (e.g., 5m)")
	flag.Parse()

	// If args not provided via flags, try positional arguments
	args := flag.Args()
	if messageSize == 0 && len(args) > 0 {
		size, err := strconv.ParseInt(args[0], 10, 64)
		if err != nil {
			log.Fatalf("Invalid message size: %v", err)
		}
		messageSize = size
	}

	if messageRate == 0 && len(args) > 1 {
		rate, err := strconv.Atoi(args[1])
		if err != nil {
			log.Fatalf("Invalid message rate: %v", err)
		}
		messageRate = rate
	}

	if runDuration == 0 && len(args) > 2 {
		duration, err := time.ParseDuration(args[2])
		if err != nil {
			log.Fatalf("Invalid duration format: %v", err)
		}
		runDuration = duration
	}

	// Verify we have all required arguments
	if messageSize <= 0 || messageRate <= 0 || runDuration <= 0 {
		log.Fatalf("Please provide valid values for size, rate, and duration.\n" +
			"Example: ./producer -size=1024 -rate=10000 -duration=5m\n" +
			"     or: ./producer 1024 10000 5m")
	}

	fmt.Printf("Starting producer with size=%d bytes, rate=%d msg/s, duration=%s\n",
		messageSize, messageRate, runDuration)

	kafkaBroker := "kafka:9092"
	topic := "test_topic"

	// Rest of your existing producer code remains unchanged
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
