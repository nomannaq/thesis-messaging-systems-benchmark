package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Result struct {
	latency time.Duration
	size    int // Size of the message in bytes
}

// MetricSnapshot includes CPU/memory fields added
type MetricSnapshot struct {
	timestamp       time.Time
	messagesCount   int
	bytesReceived   int64
	avgLatencyMs    float64
	minLatencyMs    int64
	maxLatencyMs    int64
	msgThroughput   float64
	mbThroughput    float64
	partition       int
	offset          int64
	cpuUsagePercent float64
	memUsageMB      float64
}

// Globals for CPU measurement between snapshots
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
	// Optionally free OS memory to update stats more often:
	// debug.FreeOSMemory()
	return float64(m.Alloc) / (1024.0 * 1024.0)
}

func main() {
	var runDuration time.Duration

	fmt.Print("Enter run duration (e.g., 5m for 5 minutes): ")
	var durationInput string
	fmt.Scan(&durationInput)

	// Parse the duration input (e.g., "5m" for 5 minutes)
	runDurationParsed, err := time.ParseDuration(durationInput)
	if err != nil {
		log.Fatalf("Invalid duration format: %v", err)
	}
	runDuration = runDurationParsed

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":      "localhost:29092",
		"group.id":               "perf-test-group",
		"auto.offset.reset":      "latest",
		"fetch.min.bytes":        1,
		"fetch.max.bytes":        5242880,
		"session.timeout.ms":     6000,
		"heartbeat.interval.ms":  2000,
		"statistics.interval.ms": 1000,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	consumer.SubscribeTopics([]string{"test_topic"}, nil)

	resultChan := make(chan Result, 10000)
	metricsChan := make(chan MetricSnapshot, 1000)
	workers := runtime.NumCPU() * 2 // Dynamically set based on CPU cores
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for res := range resultChan {
				// Simulate processing
				_ = res
			}
		}()
	}

	// Start metrics collector
	wg.Add(1)
	go func() {
		defer wg.Done()
		collectAndSaveMetrics(metricsChan)
	}()

	var (
		totalMsg      int
		totalLatency  time.Duration
		totalBytes    int64
		minLatency    = time.Hour
		maxLatency    time.Duration
		startTime     time.Time
		endTime       time.Time
		lastPartition int
		lastOffset    int64
	)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Create metrics ticker
	metricsTicker := time.NewTicker(1 * time.Second)
	defer metricsTicker.Stop()

	fmt.Printf("Consumer started (workers=%d)\n", workers)

	// Initialize CPU measurement references
	prevCPUTime = getCPUTime()
	prevWallTime = time.Now()

	startTime = time.Now()
	endTime = startTime.Add(runDuration)

	// Start periodic metrics collection
	go func() {
		for range metricsTicker.C {
			elapsed := time.Since(startTime).Seconds()
			if elapsed <= 0 {
				continue
			}
			// Create metrics snapshot
			snapshot := MetricSnapshot{
				timestamp:       time.Now(),
				messagesCount:   totalMsg,
				bytesReceived:   totalBytes,
				partition:       lastPartition,
				offset:          lastOffset,
				cpuUsagePercent: getCPUUsagePercent(),
				memUsageMB:      getMemoryUsageMB(),
			}

			// Calculate latency stats if we have messages
			if totalMsg > 0 {
				snapshot.avgLatencyMs = float64(totalLatency.Milliseconds()) / float64(totalMsg)
				snapshot.minLatencyMs = minLatency.Milliseconds()
				snapshot.maxLatencyMs = maxLatency.Milliseconds()
			}

			// Calculate throughput
			snapshot.msgThroughput = float64(totalMsg) / elapsed
			snapshot.mbThroughput = float64(totalBytes) / 1024 / 1024 / elapsed

			// Send metrics to channel if consumer is still running
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

			// Read a message with a 1-second timeout
			msg, err := consumer.ReadMessage(time.Second)
			if err != nil {
				// Kafka timeout
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue
				}
				log.Printf("Error: %v", err)
				continue
			}

			// Calculate latency from message timestamp
			latency := time.Since(msg.Timestamp)
			resultChan <- Result{latency: latency, size: len(msg.Value)}

			totalMsg++
			totalLatency += latency
			totalBytes += int64(len(msg.Value))
			lastPartition = int(msg.TopicPartition.Partition)
			lastOffset = int64(msg.TopicPartition.Offset)

			if latency < minLatency {
				minLatency = latency
			}
			if latency > maxLatency {
				maxLatency = latency
			}
		}
	}

	// Final metrics collection after loop ends
	if totalMsg > 0 {
		elapsed := time.Since(startTime).Seconds()
		finalSnapshot := MetricSnapshot{
			timestamp:       time.Now(),
			messagesCount:   totalMsg,
			bytesReceived:   totalBytes,
			partition:       lastPartition,
			offset:          lastOffset,
			cpuUsagePercent: getCPUUsagePercent(),
			memUsageMB:      getMemoryUsageMB(),
		}

		finalSnapshot.avgLatencyMs = float64(totalLatency.Milliseconds()) / float64(totalMsg)
		finalSnapshot.minLatencyMs = minLatency.Milliseconds()
		finalSnapshot.maxLatencyMs = maxLatency.Milliseconds()
		finalSnapshot.msgThroughput = float64(totalMsg) / elapsed
		finalSnapshot.mbThroughput = float64(totalBytes) / 1024 / 1024 / elapsed

		metricsChan <- finalSnapshot
	}

	// Close channels and wait for goroutines to finish
	close(resultChan)
	close(metricsChan)
	wg.Wait()

	elapsed := time.Since(startTime)
	// Print summary to console
	fmt.Printf("\n=== Consumer Metrics ===\n")
	fmt.Printf("Run Duration: %s\n", runDuration)
	fmt.Printf("Messages received: %d\n", totalMsg)
	fmt.Printf("Time elapsed: %.2f seconds\n", elapsed.Seconds())

	if totalMsg > 0 && elapsed.Seconds() > 0 {
		fmt.Printf("Throughput: %.2f messages/second\n", float64(totalMsg)/elapsed.Seconds())
		fmt.Printf("Throughput: %.2f MB/second\n", float64(totalBytes)/1024/1024/elapsed.Seconds())
		avgLatency := time.Duration(int64(totalLatency) / int64(totalMsg))
		fmt.Printf("Latency (min/mean/max): %v / %v / %v\n",
			minLatency.Round(time.Microsecond),
			avgLatency.Round(time.Microsecond),
			maxLatency.Round(time.Microsecond))
		fmt.Printf("Last partition/offset: %d/%d\n", lastPartition, lastOffset)
	} else {
		fmt.Println("No messages were received during the test run.")
	}

	fmt.Println("Metrics have been saved to CSV file.")
	os.Exit(0)
}

// collectAndSaveMetrics saves metrics snapshots to a CSV file,
// now including CPU and memory usage.
func collectAndSaveMetrics(metricsChan chan MetricSnapshot) {
	filename := fmt.Sprintf("kafka_consumer_metrics_%s.csv", time.Now().Format("2006-01-02_15-04-05"))
	file, err := os.Create(filename)
	if err != nil {
		log.Printf("Failed to create metrics file: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header, including new fields
	headers := []string{
		"timestamp",
		"messages_count",
		"bytes_received",
		"msg_throughput",
		"mb_throughput",
		"avg_latency_ms",
		"min_latency_ms",
		"max_latency_ms",
		"partition",
		"offset",
		"cpu_usage_percent",
		"mem_usage_mb",
	}
	if err := writer.Write(headers); err != nil {
		log.Printf("Error writing CSV headers: %v", err)
		return
	}

	// Write metrics data from channel as they arrive
	for m := range metricsChan {
		row := []string{
			m.timestamp.Format(time.RFC3339),
			strconv.Itoa(m.messagesCount),
			strconv.FormatInt(m.bytesReceived, 10),
			fmt.Sprintf("%.2f", m.msgThroughput),
			fmt.Sprintf("%.2f", m.mbThroughput),
			fmt.Sprintf("%.2f", m.avgLatencyMs),
			fmt.Sprintf("%d", m.minLatencyMs),
			fmt.Sprintf("%d", m.maxLatencyMs),
			strconv.Itoa(m.partition),
			strconv.FormatInt(m.offset, 10),
			fmt.Sprintf("%.2f", m.cpuUsagePercent),
			fmt.Sprintf("%.2f", m.memUsageMB),
		}

		if err := writer.Write(row); err != nil {
			log.Printf("Error writing CSV row: %v", err)
			continue
		}
		writer.Flush()
	}

	log.Printf("Metrics exported to %s", filename)
}
