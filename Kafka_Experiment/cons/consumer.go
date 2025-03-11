package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Result struct {
	latency time.Duration
	size    int // Size of the message in bytes
}

func main() {
	var runDuration time.Duration

	fmt.Print("Enter run duration (e.g., 5m for 5 minutes): ")
	var durationInput string
	fmt.Scan(&durationInput)

	// Parse the duration input (e.g., "5m" for 5 minutes)
	runDuration, err := time.ParseDuration(durationInput)
	if err != nil {
		log.Fatalf("Invalid duration format: %v", err)
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     "localhost:29092",
		"group.id":              "perf-test-group",
		"auto.offset.reset":     "latest", // Change to "latest" to avoid old messages
		"fetch.min.bytes":       1,        // Fetch messages as soon as they arrive
		"fetch.max.bytes":       5242880,  // 5MB
		"session.timeout.ms":    6000,     // Prevent frequent rebalancing
		"heartbeat.interval.ms": 2000,     // Keep heartbeats frequent
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	consumer.SubscribeTopics([]string{"test_topic"}, nil)

	resultChan := make(chan Result, 10000)
	workers := runtime.NumCPU() * 2 // Dynamically set based on CPU cores
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for res := range resultChan {
				_ = res // Simulate processing
			}
		}()
	}

	var (
		totalMsg     int
		totalLatency time.Duration
		totalBytes   int64
		minLatency   = time.Hour
		maxLatency   time.Duration
		startTime    time.Time
		endTime      time.Time
	)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("Consumer started (workers=%d)\n", workers)

	startTime = time.Now()
	endTime = startTime.Add(runDuration) // Calculate end time

ConsumerLoop:
	for {
		select {
		case sig := <-sigchan:
			fmt.Printf("Terminating: %v\n", sig)
			break ConsumerLoop
		default:
			if time.Now().After(endTime) {
				fmt.Println("Run duration reached. Stopping consumer.")
				break ConsumerLoop
			}

			msg, err := consumer.ReadMessage(time.Second) // Increase timeout to 1s
			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue // Avoid unnecessary logs
				}
				log.Printf("Error: %v", err)
				continue
			}

			latency := time.Since(msg.Timestamp)
			resultChan <- Result{latency: latency, size: len(msg.Value)}

			totalMsg++
			totalLatency += latency
			totalBytes += int64(len(msg.Value))
			if latency < minLatency {
				minLatency = latency
			}
			if latency > maxLatency {
				maxLatency = latency
			}
		}
	}

	close(resultChan)
	wg.Wait()
	elapsed := time.Since(startTime)

	fmt.Printf("\n=== Consumer Metrics ===\n")
	fmt.Printf("Run Duration: %s\n", runDuration)
	fmt.Printf("Messages received: %d\n", totalMsg)
	fmt.Printf("Time elapsed: %.2f seconds\n", elapsed.Seconds())
	fmt.Printf("Throughput: %.2f messages/second\n", float64(totalMsg)/elapsed.Seconds())
	fmt.Printf("Throughput: %.2f MB/second\n", float64(totalBytes)/1024/1024/elapsed.Seconds())

	avgLatency := time.Duration(int64(totalLatency) / int64(totalMsg))
	fmt.Printf("Latency (min/mean/max): %v / %v / %v\n",
		minLatency.Round(time.Microsecond),
		avgLatency.Round(time.Microsecond),
		maxLatency.Round(time.Microsecond))

	os.Exit(0)
}
