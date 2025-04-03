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

	pulsar "github.com/apache/pulsar-client-go/pulsar"
)

type Result struct {
	latency time.Duration
	size    int
}

func main() {
	var runDuration time.Duration

	fmt.Print("Enter run duration (e.g., 5m for 5 minutes): ")
	var durationInput string
	fmt.Scan(&durationInput)

	// Parse duration input
	runDuration, err := time.ParseDuration(durationInput)
	if err != nil {
		log.Fatalf("Invalid duration format: %v", err)
	}

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            "test_topic",
		SubscriptionName: "perf-test-subscription",
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	resultChan := make(chan Result, 10000)
	workers := runtime.NumCPU() * 2
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
		totalMsg     atomic.Int64
		totalLatency atomic.Int64
		totalBytes   atomic.Int64
		minLatency   atomic.Int64
		maxLatency   atomic.Int64
		startTime    = time.Now()
		endTime      = startTime.Add(runDuration)
	)

	// Initialize minLatency with maximum possible value
	minLatency.Store(1<<63 - 1)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("Consumer started (workers=%d)\n", workers)

	ctx, cancel := context.WithDeadline(context.Background(), endTime)
	defer cancel()

ConsumerLoop:
	for {
		select {
		case sig := <-sigchan: // Fixed: capture the signal value
			fmt.Printf("Terminating: %v\n", sig)
			break ConsumerLoop
		default:
			if time.Now().After(endTime) {
				fmt.Println("Run duration reached. Stopping consumer.")
				break ConsumerLoop
			}

			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			msg, err := consumer.Receive(ctx)
			cancel()

			if err != nil {
				if err == context.DeadlineExceeded {
					continue
				}
				log.Printf("Error receiving message: %v", err)
				break ConsumerLoop
			}

			latency := time.Since(msg.EventTime())
			resultChan <- Result{
				latency: latency,
				size:    len(msg.Payload()),
			}

			totalMsg.Add(1)
			totalBytes.Add(int64(len(msg.Payload())))
			latencyNs := int64(latency)

			// Update min latency
			for {
				currentMin := minLatency.Load()
				if latencyNs >= currentMin {
					break
				}
				if minLatency.CompareAndSwap(currentMin, latencyNs) {
					break
				}
			}

			// Update max latency
			for {
				currentMax := maxLatency.Load()
				if latencyNs <= currentMax {
					break
				}
				if maxLatency.CompareAndSwap(currentMax, latencyNs) {
					break
				}
			}

			totalLatency.Add(latencyNs)
			consumer.Ack(msg)
		}
	}

	close(resultChan)
	wg.Wait()
	elapsed := time.Since(startTime)

	fmt.Printf("\n=== Consumer Metrics ===\n")
	fmt.Printf("Run Duration: %s\n", runDuration)
	fmt.Printf("Messages received: %d\n", totalMsg.Load())
	fmt.Printf("Time elapsed: %.2f seconds\n", elapsed.Seconds())
	fmt.Printf("Throughput: %.2f messages/second\n", float64(totalMsg.Load())/elapsed.Seconds())
	fmt.Printf("Throughput: %.2f MB/second\n", float64(totalBytes.Load())/1024/1024/elapsed.Seconds())

	if totalMsg.Load() > 0 {
		avgLatency := time.Duration(totalLatency.Load() / totalMsg.Load())
		fmt.Printf("Latency (min/mean/max): %s / %s / %s\n",
			time.Duration(minLatency.Load()).Round(time.Microsecond),
			avgLatency.Round(time.Microsecond),
			time.Duration(maxLatency.Load()).Round(time.Microsecond))
	}

	os.Exit(0)
}
