package main

import (
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
)

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("Usage: go run producer.go <messageSize> <duration> (e.g., 1024 1m)")
	}
	messageSize, err := strconv.ParseInt(os.Args[1], 10, 64)
	if err != nil {
		log.Fatalf("Invalid message size: %v", err)
	}
	runDuration, err := time.ParseDuration(os.Args[2])
	if err != nil {
		log.Fatalf("Invalid duration format: %v", err)
	}

	kafkaBroker := "localhost:29092"
	topic := "test_topic"

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBroker,
		"linger.ms":         0,
		"batch.size":        64000,
		"compression.type":  "lz4",
		"acks":              "all",
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
		workers      = runtime.NumCPU() * 2
		shutdown     = make(chan struct{})
		start        = time.Now()
		endTime      = start.Add(runDuration)
	)

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

					err := producer.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
						Value:          payload,
						Timestamp:      time.Now().UTC(),
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

	go func() {
		for range deliveryChan {
		}
	}()

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
