package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"golang.org/x/time/rate"
)

func main() {
	var messageSize, totalMessages int64
	var messageRate int

	fmt.Print("Enter message size (bytes): ")
	fmt.Scan(&messageSize)
	fmt.Print("Enter message rate (messages/sec): ")
	fmt.Scan(&messageRate)
	fmt.Print("Enter total messages to send: ")
	fmt.Scan(&totalMessages)

	kafkaBroker := "localhost:29092"
	topic := "test_topic"

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBroker,
		"linger.ms":         10,    // Small batching
		"batch.size":        16384, // 16KB batch size
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
		workers      = 16
		messagesLeft = totalMessages
		start        = time.Now()
	)

	// Start delivery report handler
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					failureCount.Add(1)
				} else {
					successCount.Add(1)
				}
			}
		}
	}()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for atomic.LoadInt64(&messagesLeft) > 0 {
				if err := limiter.WaitN(context.Background(), 1); err != nil {
					log.Printf("Rate limiter error: %v", err)
					continue
				}

				if atomic.AddInt64(&messagesLeft, -1) < 0 {
					return
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
		}()
	}

	wg.Wait()
	producer.Flush(30 * 1000)
	close(deliveryChan)
	elapsed := time.Since(start)

	fmt.Printf("\n=== Producer Metrics ===\n")
	fmt.Printf("Attempted: %d\n", totalMessages)
	fmt.Printf("Successful: %d\n", successCount.Load())
	fmt.Printf("Failed: %d\n", failureCount.Load())
	fmt.Printf("Throughput: %.2f msg/s\n", float64(successCount.Load())/elapsed.Seconds())
	fmt.Printf("Throughput: %.2f MB/s\n",
		(float64(successCount.Load()*messageSize) / 1024 / 1024 / elapsed.Seconds()))
}
