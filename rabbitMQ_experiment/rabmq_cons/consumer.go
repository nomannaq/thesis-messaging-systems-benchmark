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

	"github.com/streadway/amqp"
)

type Result struct {
	latency time.Duration
	size    int
}

type MetricSnapshot struct {
	timestamp       time.Time
	messagesCount   int
	bytesReceived   int64
	avgLatencyMs    float64
	minLatencyMs    int64
	maxLatencyMs    int64
	msgThroughput   float64
	mbThroughput    float64
	cpuUsagePercent float64
	memUsageMB      float64
}

var (
	prevCPUTime  time.Duration
	prevWallTime time.Time
)

func getCPUTime() time.Duration {
	var rusage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		return 0
	}
	user := time.Duration(rusage.Utime.Sec)*time.Second + time.Duration(rusage.Utime.Usec)*time.Microsecond
	sys := time.Duration(rusage.Stime.Sec)*time.Second + time.Duration(rusage.Stime.Usec)*time.Microsecond
	return user + sys
}

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
	return float64(cpuDelta) / float64(wallDelta) * 100.0
}

func getMemoryUsageMB() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.Alloc) / (1024.0 * 1024.0)
}

func collectAndSaveMetrics(metricsChan chan MetricSnapshot) {
	filename := fmt.Sprintf("rabbitmq_consumer_metrics_%s.csv", time.Now().Format("2006-01-02_15-04-05"))
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

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: go run consumer.go <duration> (e.g., 1m)")
	}
	runDuration, err := time.ParseDuration(os.Args[1])
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

	msgs, err := ch.Consume(
		queueName, "", true, false, false, false, nil,
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	resultChan := make(chan Result, 10000)
	metricsChan := make(chan MetricSnapshot, 1000)
	workers := runtime.NumCPU() * 2
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for res := range resultChan {
				_ = res
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		collectAndSaveMetrics(metricsChan)
	}()

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

	metricsTicker := time.NewTicker(1 * time.Second)
	defer metricsTicker.Stop()

	fmt.Printf("Consumer started (workers=%d)\n", workers)

	prevCPUTime = getCPUTime()
	prevWallTime = time.Now()

	startTime = time.Now()
	endTime = startTime.Add(runDuration)

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
			select {
			case msg := <-msgs:
				latency := time.Since(msg.Timestamp)
				resultChan <- Result{latency: latency, size: len(msg.Body)}

				totalMsg++
				totalLatency += latency
				totalBytes += int64(len(msg.Body))
				if latency < minLatency {
					minLatency = latency
				}
				if latency > maxLatency {
					maxLatency = latency
				}
			case <-time.After(100 * time.Millisecond):
			}
		}
	}

	if totalMsg > 0 {
		elapsed := time.Since(startTime).Seconds()
		finalSnapshot := MetricSnapshot{
			timestamp:       time.Now(),
			messagesCount:   totalMsg,
			bytesReceived:   totalBytes,
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
	} else {
		fmt.Println("No messages were received during the test run.")
	}

	fmt.Println("Metrics have been saved to CSV file.")
	os.Exit(0)
}
