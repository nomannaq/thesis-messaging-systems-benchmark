package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
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
	var batchSize int
	fmt.Print("Enter batch size: ")
	fmt.Scan(&batchSize)

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
		"group.id":          "perf-test-group",
		"auto.offset.reset": "earliest",
		"fetch.min.bytes":   1048576, // 1MB
		"fetch.max.bytes":   5242880, // 5MB
		//"max.poll.records":  10000,   // Process 10k messages per poll
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	consumer.SubscribeTopics([]string{"test_topic"}, nil)

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
			msg, err := consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue
				}
				log.Printf("Error: %v", err)
				continue
			}

			// Start the timer when the first message is received
			if totalMsg == 0 {
				startTime = time.Now()
			}

			latency := time.Since(msg.Timestamp)
			resultChan <- Result{latency: latency, size: len(msg.Value)}

			totalMsg++
			totalLatency += latency
			totalBytes += int64(len(msg.Value)) // Accumulate total bytes
			if latency < minLatency {
				minLatency = latency
			}
			if latency > maxLatency {
				maxLatency = latency
			}

			if totalMsg >= batchSize {
				break BatchLoop
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
