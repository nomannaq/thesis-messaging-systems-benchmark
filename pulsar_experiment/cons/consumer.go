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

// Result represents a single consumed message
type Result struct {
	latency time.Duration
	size    int
}

// MetricSnapshot now includes CPU/memory fields
type MetricSnapshot struct {
	timestamp       time.Time
	messagesCount   int64
	bytesReceived   int64
	avgLatencyMs    float64
	minLatencyMs    int64
	maxLatencyMs    int64
	msgThroughput   float64
	mbThroughput    float64
	cpuUsagePercent float64
	memUsageMB      float64
	partition       int
	offset          int64
}

// Globals for CPU usage tracking
var (
	prevCPUTime  time.Duration
	prevWallTime time.Time
)

// getCPUTime returns total user+system CPU time of the calling process.
func getCPUTime() time.Duration {
	var rusage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		return 0
	}
	user := time.Duration(rusage.Utime.Sec)*time.Second + time.Duration(rusage.Utime.Usec)*time.Microsecond
	sys := time.Duration(rusage.Stime.Sec)*time.Second + time.Duration(rusage.Stime.Usec)*time.Microsecond
	return user + sys
}

// getCPUUsagePercent calculates CPU usage since the last call.
func getCPUUsagePercent() float64 {
	// Current CPU & wall-clock times
	currCPUTime := getCPUTime()
	currWallTime := time.Now()

	if prevWallTime.IsZero() {
		// First call, just initialise
		prevCPUTime = currCPUTime
		prevWallTime = currWallTime
		return 0.0
	}

	cpuDelta := currCPUTime - prevCPUTime
	wallDelta := currWallTime.Sub(prevWallTime)

	// Update previous for next calculation
	prevCPUTime = currCPUTime
	prevWallTime = currWallTime

	if wallDelta <= 0 {
		return 0.0
	}
	return float64(cpuDelta) / float64(wallDelta) * 100.0
}

// getMemoryUsageMB returns the current allocated memory in MB.
func getMemoryUsageMB() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// Optionally force a GC to refresh stats:
	// debug.FreeOSMemory()
	return float64(m.Alloc) / (1024.0 * 1024.0)
}

// collectAndSaveMetrics writes metrics (including CPU/memory) to a CSV file.
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
		"cpu_usage_percent",
		"mem_usage_mb",
	}
	if err := writer.Write(headers); err != nil {
		log.Printf("Error writing CSV headers: %v", err)
		return
	}

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
			fmt.Sprintf("%.2f", snapshot.cpuUsagePercent),
			fmt.Sprintf("%.2f", snapshot.memUsageMB),
		}
		if err := writer.Write(row); err != nil {
			log.Printf("Error writing CSV row: %v", err)
		}
		writer.Flush()
	}
	log.Printf("Metrics exported to %s", filename)
}

func main() {
	var runDuration time.Duration

	fmt.Print("Enter run duration (e.g., 5m for 5 minutes): ")
	var durationInput string
	fmt.Scan(&durationInput)

	// Parse run duration
	parsedDuration, err := time.ParseDuration(durationInput)
	if err != nil {
		log.Fatalf("Invalid duration format: %v", err)
	}
	runDuration = parsedDuration

	// Create Pulsar client
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Subscribe to topic
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       "test_topic",
		SubscriptionName:            "perf-test-subscription",
		Type:                        pulsar.Shared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest, // Similar to "auto.offset.reset: latest"
		ReceiverQueueSize:           10000,
		Name:                        "performance-consumer",
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
	minLatency.Store(1<<63 - 1) // Initialize to a large value

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	fmt.Printf("Pulsar consumer started (workers=%d)\n", workers)

	ctx, cancel := context.WithDeadline(context.Background(), endTime)
	defer cancel()

	// Channel and goroutine for metrics
	metricsChan := make(chan MetricSnapshot, 1000)
	wg.Add(1)
	go func() {
		defer wg.Done()
		collectAndSaveMetrics(metricsChan)
	}()

	// CPU usage reference initialization
	prevCPUTime = getCPUTime()
	prevWallTime = time.Now()

	// Periodic metrics
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	go func() {
		for range ticker.C {
			elapsed := time.Since(startTime).Seconds()
			if elapsed <= 0 {
				continue
			}
			cnt := totalMsg.Load()
			if cnt == 0 {
				// Still send a snapshot with zero counts
				metricsChan <- MetricSnapshot{
					timestamp:       time.Now(),
					messagesCount:   0,
					bytesReceived:   0,
					cpuUsagePercent: getCPUUsagePercent(),
					memUsageMB:      getMemoryUsageMB(),
				}
				continue
			}
			avgLat := float64(totalLatency.Load()/cnt) / 1e6
			snapshot := MetricSnapshot{
				timestamp:       time.Now(),
				messagesCount:   cnt,
				bytesReceived:   totalBytes.Load(),
				avgLatencyMs:    avgLat,
				minLatencyMs:    minLatency.Load() / 1e6,
				maxLatencyMs:    maxLatency.Load() / 1e6,
				msgThroughput:   float64(cnt) / elapsed,
				mbThroughput:    float64(totalBytes.Load()) / (1024 * 1024) / elapsed,
				cpuUsagePercent: getCPUUsagePercent(),
				memUsageMB:      getMemoryUsageMB(),
			}
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
			ctxT, cancelT := context.WithTimeout(ctx, time.Second)
			msg, err := consumer.Receive(ctxT)
			cancelT()
			if err != nil {
				if err == context.DeadlineExceeded {
					continue
				}
				log.Printf("Error receiving message: %v", err)
				break ConsumerLoop
			}
			consumer.Ack(msg)

			payloadSize := len(msg.Payload())
			lat := time.Since(msg.EventTime())

			resultChan <- Result{latency: lat, size: payloadSize}
			totalMsg.Add(1)
			totalBytes.Add(int64(payloadSize))
			latencyNs := int64(lat)
			totalLatency.Add(latencyNs)

			// Check min
			for {
				currentMin := minLatency.Load()
				if latencyNs >= currentMin {
					break
				}
				if minLatency.CompareAndSwap(currentMin, latencyNs) {
					break
				}
			}
			// Check max
			for {
				currentMax := maxLatency.Load()
				if latencyNs <= currentMax {
					break
				}
				if maxLatency.CompareAndSwap(currentMax, latencyNs) {
					break
				}
			}
		}
	}

	close(resultChan)
	wg.Wait() // Ensure workers are done

	finalCount := totalMsg.Load()
	if finalCount > 0 {
		elapsed := time.Since(startTime).Seconds()
		snapshot := MetricSnapshot{
			timestamp:       time.Now(),
			messagesCount:   finalCount,
			bytesReceived:   totalBytes.Load(),
			cpuUsagePercent: getCPUUsagePercent(),
			memUsageMB:      getMemoryUsageMB(),
		}
		snapshot.avgLatencyMs = float64(totalLatency.Load()/finalCount) / 1e6
		snapshot.minLatencyMs = minLatency.Load() / 1e6
		snapshot.maxLatencyMs = maxLatency.Load() / 1e6
		snapshot.msgThroughput = float64(finalCount) / elapsed
		snapshot.mbThroughput = float64(totalBytes.Load()) / (1024 * 1024) / elapsed
		metricsChan <- snapshot
	}

	close(metricsChan)

	duration := time.Since(startTime)
	fmt.Printf("\n=== Consumer Metrics ===\n")
	fmt.Printf("Run Duration: %s\n", runDuration)
	fmt.Printf("Messages received: %d\n", finalCount)
	fmt.Printf("Time elapsed: %.2f seconds\n", duration.Seconds())

	if finalCount > 0 && duration.Seconds() > 0 {
		fmt.Printf("Throughput: %.2f messages/second\n", float64(finalCount)/duration.Seconds())
		fmt.Printf("Throughput: %.2f MB/second\n", float64(totalBytes.Load())/(1024*1024)/duration.Seconds())
		avgLat := time.Duration(totalLatency.Load() / finalCount)
		fmt.Printf("Latency (min/mean/max): %v / %v / %v\n",
			time.Duration(minLatency.Load()).Round(time.Microsecond),
			avgLat.Round(time.Microsecond),
			time.Duration(maxLatency.Load()).Round(time.Microsecond))
	} else {
		fmt.Println("No messages were received during the test run.")
	}

	fmt.Println("Metrics have been saved to CSV file.")
	os.Exit(0)
}
