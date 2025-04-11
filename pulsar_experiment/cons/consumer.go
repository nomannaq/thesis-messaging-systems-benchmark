package main

import (
	"context"
	"encoding/csv"
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

	pulsar "github.com/apache/pulsar-client-go/pulsar"
)

// ...existing code...

type Result struct {
	latency time.Duration
	size    int
}

// Add a new struct for metrics snapshots
type MetricSnapshot struct {
	timestamp     time.Time
	messagesCount int64
	bytesReceived int64
	avgLatencyMs  float64
	minLatencyMs  int64
	maxLatencyMs  int64
	msgThroughput float64
	mbThroughput  float64
}

// collectAndSaveMetrics saves metrics snapshots to a CSV file
func collectAndSaveMetrics(metricsChan chan MetricSnapshot) {
	filename := fmt.Sprintf("pulsar_consumer_metrics_%s.csv", time.Now().Format("2006-01-02_15-04-05"))
	file, err := os.Create(filename)
	if err != nil {
		log.Printf("Failed to create metrics file: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write CSV headers
	headers := []string{
		"timestamp",
		"messages_count",
		"bytes_received",
		"msg_throughput",
		"mb_throughput",
		"avg_latency_ms",
		"min_latency_ms",
		"max_latency_ms",
	}
	if err := writer.Write(headers); err != nil {
		log.Printf("Error writing CSV headers: %v", err)
		return
	}

	// Pull snapshots from the channel and write rows as they're produced
	for snapshot := range metricsChan {
		row := []string{
			snapshot.timestamp.Format(time.RFC3339),
			strconv.FormatInt(snapshot.messagesCount, 10),
			strconv.FormatInt(snapshot.bytesReceived, 10),
			fmt.Sprintf("%.2f", snapshot.msgThroughput),
			fmt.Sprintf("%.2f", snapshot.mbThroughput),
			fmt.Sprintf("%.2f", snapshot.avgLatencyMs),
			fmt.Sprintf("%d", snapshot.minLatencyMs),
			fmt.Sprintf("%d", snapshot.maxLatencyMs),
		}
		if err := writer.Write(row); err != nil {
			log.Printf("Error writing CSV row: %v", err)
		}
		writer.Flush()
	}
	log.Printf("Metrics exported to %s", filename)
}

// ...existing code...

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

	// Use atomic counters for thread-safe metrics
	var (
		totalMsg     atomic.Int64
		totalLatency atomic.Int64
		totalBytes   atomic.Int64
		minLatency   atomic.Int64
		maxLatency   atomic.Int64
		startTime    = time.Now()
		endTime      = startTime.Add(runDuration)
	)

	// Initialize minLatency with a very large value
	minLatency.Store(1<<63 - 1)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("Consumer started (workers=%d)\n", workers)

	ctx, cancel := context.WithDeadline(context.Background(), endTime)
	defer cancel()

	// Create a channel for metric snapshots
	metricsChan := make(chan MetricSnapshot, 1000)

	// Start a goroutine to save snapshots to CSV
	wg.Add(1)
	go func() {
		defer wg.Done()
		collectAndSaveMetrics(metricsChan)
	}()

	// Create a ticker to generate periodic snapshots
	metricsTicker := time.NewTicker(1 * time.Second)
	defer metricsTicker.Stop()

	// Periodic metrics collection
	go func() {
		for range metricsTicker.C {
			// Calculate elapsed time
			elapsed := time.Since(startTime).Seconds()
			if elapsed <= 0 {
				continue
			}

			msgCount := float64(totalMsg.Load())
			snapshot := MetricSnapshot{
				timestamp:     time.Now(),
				messagesCount: totalMsg.Load(),
				bytesReceived: totalBytes.Load(),
			}

			if msgCount > 0 {
				// Convert latency from ns to ms
				snapshot.avgLatencyMs = float64(totalLatency.Load()/(int64(msgCount))) / 1e6
				snapshot.minLatencyMs = minLatency.Load() / 1e6
				snapshot.maxLatencyMs = maxLatency.Load() / 1e6
			}

			// Calculate throughput
			snapshot.msgThroughput = msgCount / elapsed
			snapshot.mbThroughput = float64(totalBytes.Load()) / (1024 * 1024) / elapsed

			// Publish snapshot if still running
			if time.Now().Before(endTime) {
				metricsChan <- snapshot
			}
		}
	}()

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

			ctxT, cancelT := context.WithTimeout(ctx, 1*time.Second)
			msg, err := consumer.Receive(ctxT)
			cancelT()

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

			// Update min latency if smaller
			for {
				currentMin := minLatency.Load()
				if latencyNs >= currentMin {
					break
				}
				if minLatency.CompareAndSwap(currentMin, latencyNs) {
					break
				}
			}

			// Update max latency if larger
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
	wg.Wait() // Wait for workers to drain resultChan first

	// Final snapshot if messages were received
	finalCount := totalMsg.Load()
	if finalCount > 0 {
		elapsed := time.Since(startTime).Seconds()
		snapshot := MetricSnapshot{
			timestamp:     time.Now(),
			messagesCount: finalCount,
			bytesReceived: totalBytes.Load(),
		}
		snapshot.avgLatencyMs = float64(totalLatency.Load()/finalCount) / 1e6
		snapshot.minLatencyMs = minLatency.Load() / 1e6
		snapshot.maxLatencyMs = maxLatency.Load() / 1e6
		snapshot.msgThroughput = float64(finalCount) / elapsed
		snapshot.mbThroughput = float64(totalBytes.Load()) / (1024 * 1024) / elapsed

		metricsChan <- snapshot
	}

	// Close metrics channel so CSV goroutine can exit
	close(metricsChan)

	elapsed := time.Since(startTime)
	fmt.Printf("\n=== Consumer Metrics ===\n")
	fmt.Printf("Run Duration: %s\n", runDuration)
	fmt.Printf("Messages received: %d\n", finalCount)
	fmt.Printf("Time elapsed: %.2f seconds\n", elapsed.Seconds())

	if finalCount > 0 && elapsed.Seconds() > 0 {
		fmt.Printf("Throughput: %.2f messages/second\n", float64(finalCount)/elapsed.Seconds())
		fmt.Printf("Throughput: %.2f MB/second\n", float64(totalBytes.Load())/1024/1024/elapsed.Seconds())

		avgLatency := time.Duration(totalLatency.Load() / finalCount)
		fmt.Printf("Latency (min/mean/max): %v / %v / %v\n",
			time.Duration(minLatency.Load()).Round(time.Microsecond),
			avgLatency.Round(time.Microsecond),
			time.Duration(maxLatency.Load()).Round(time.Microsecond))
	} else {
		fmt.Println("No messages were received during the test run.")
	}

	fmt.Println("Metrics have been saved to CSV file.")
	os.Exit(0)
}
