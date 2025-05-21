package main

import (
	"encoding/csv"
	"flag"
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

// waitForTopic waits for topic to become available
func waitForTopic(consumer *kafka.Consumer, topic string, maxAttempts int) bool {
	log.Printf("Waiting for topic %s to become available...", topic)
	for i := 0; i < maxAttempts; i++ {
		metadata, err := consumer.GetMetadata(&topic, false, 5000)
		if err == nil {
			if topicMetadata, ok := metadata.Topics[topic]; ok && len(topicMetadata.Partitions) > 0 {
				log.Printf("Topic %s is available with %d partitions", topic, len(topicMetadata.Partitions))
				return true
			}
		}
		log.Printf("Waiting for topic %s (attempt %d/%d): %v", topic, i+1, maxAttempts, err)
		time.Sleep(2 * time.Second)
	}
	log.Printf("Topic %s not available after %d attempts", topic, maxAttempts)
	return false
}

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

// getCPUUsagePercent calculates CPU usage since the last call (single-core-based).
func getCPUUsagePercent() float64 {
	currCPUTime := getCPUTime()
	currWallTime := time.Now()

	if prevWallTime.IsZero() {
		prevCPUTime = currCPUTime
		prevWallTime = currWallTime
		return 0.0
	}

	cpuDelta := currCPUTime - prevCPUTime
	wallDelta := currWallTime.Sub(prevWallTime)

	prevCPUTime = currCPUTime
	prevWallTime = currWallTime

	if wallDelta <= 0 {
		return 0.0
	}
	return (float64(cpuDelta) / float64(wallDelta) * 100.0) / float64(runtime.NumCPU())
}

// getMemoryUsageMB returns the current allocated memory in MB.
func getMemoryUsageMB() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.Alloc) / (1024.0 * 1024.0)
}

func main() {
	var runDuration time.Duration

	// Accept run duration as a command-line argument or flag
	flag.DurationVar(&runDuration, "duration", 0, "Run duration (e.g., 5m, 30s)")
	flag.Parse()
	if runDuration == 0 && flag.NArg() > 0 {
		var err error
		runDuration, err = time.ParseDuration(flag.Arg(0))
		if err != nil {
			log.Fatalf("Invalid duration format: %v", err)
		}
	}
	if runDuration == 0 {
		log.Fatalf("Please provide run duration as a flag or argument (e.g., go run consumer.go -duration=5m or go run consumer.go 5m)")
	}

	// Get Kafka broker from environment variable
	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092" // Default for local development
	}
	log.Printf("Using Kafka broker: %s", kafkaBroker)

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":      kafkaBroker,
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

	// Wait for topic to be available before subscribing
	topic := "test_topic"
	if !waitForTopic(consumer, topic, 30) { // Wait up to 1 minute
		log.Fatalf("Topic %s not available after waiting, exiting", topic)
	}

	consumer.SubscribeTopics([]string{topic}, nil)

	resultChan := make(chan Result, 10000)
	metricsChan := make(chan MetricSnapshot, 1000)
	workers := runtime.NumCPU() * 2
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for res := range resultChan {
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

	metricsTicker := time.NewTicker(1 * time.Second)
	defer metricsTicker.Stop()

	fmt.Printf("Consumer started (workers=%d)\n", workers)

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
			snapshot := MetricSnapshot{
				timestamp:       time.Now(),
				messagesCount:   totalMsg,
				bytesReceived:   totalBytes,
				partition:       lastPartition,
				offset:          lastOffset,
				cpuUsagePercent: getCPUUsagePercent(),
				memUsageMB:      getMemoryUsageMB(),
			}
			if totalMsg > 0 {
				snapshot.avgLatencyMs = float64(totalLatency.Milliseconds()) / float64(totalMsg)
				snapshot.minLatencyMs = minLatency.Milliseconds()
				snapshot.maxLatencyMs = maxLatency.Milliseconds()
			}
			snapshot.msgThroughput = float64(totalMsg) / elapsed
			snapshot.mbThroughput = float64(totalBytes) / 1024 / 1024 / elapsed

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
			msg, err := consumer.ReadMessage(time.Second)
			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue
				}
				log.Printf("Error: %v", err)
				continue
			}
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

	close(resultChan)
	close(metricsChan)
	wg.Wait()

	elapsed := time.Since(startTime)
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
	// Save to mounted volume directory for persistence
	filename := fmt.Sprintf("/app/metrics/kafka_consumer_metrics_%s.csv", time.Now().Format("2006-01-02_15-04-05"))
	file, err := os.Create(filename)
	if err != nil {
		log.Printf("Failed to create metrics file: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

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
	// At the end of main() after writing metrics
	log.Println("Sleeping for 3 seconds to ensure file is written...")
	time.Sleep(3 * time.Second)
	log.Println("Consumer exiting")
	log.Printf("Metrics exported to %s", filename)
}
