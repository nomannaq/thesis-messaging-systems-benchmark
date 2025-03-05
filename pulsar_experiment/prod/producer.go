package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
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

	pulsarURL := "pulsar://localhost:6650"
	topic := "persistent://public/default/test_topic"

	// Create Pulsar client
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               pulsarURL,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not create Pulsar client: %v", err)
	}
	defer client.Close()

	// Create producer with similar settings to Kafka
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:                   topic,
		BatchingMaxPublishDelay: 10 * time.Millisecond, // Similar to linger.ms
		BatchingMaxSize:         16384,                 // 16KB batch size similar to batch.size
		BatchingMaxMessages:     1000,
		CompressionType:         pulsar.LZ4, // LZ4 compression
		SendTimeout:             10 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not create producer: %v", err)
	}
	defer producer.Close()

	// Create payload of specified size
	payload := make([]byte, messageSize)
	for i := range payload {
		payload[i] = 'A'
	}

	var (
		wg           sync.WaitGroup
		successCount atomic.Int64
		failureCount atomic.Int64
		limiter      = rate.NewLimiter(rate.Limit(messageRate), messageRate)
		workers      = 16
		messagesLeft = totalMessages
		start        = time.Now()
	)

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

				// Send message asynchronously
				producer.SendAsync(context.Background(), &pulsar.ProducerMessage{
					Payload:   payload,
					EventTime: time.Now().UTC(),
				}, func(messageID pulsar.MessageID, producerMessage *pulsar.ProducerMessage, err error) {
					if err != nil {
						failureCount.Add(1)
						log.Printf("Failed to publish message: %v", err)
					} else {
						successCount.Add(1)
					}
				})
			}
		}()
	}

	wg.Wait()

	// Ensure all messages have been delivered before calculating metrics
	// Wait for a bit to collect the final results from callbacks
	time.Sleep(1 * time.Second)

	elapsed := time.Since(start)

	fmt.Printf("\n=== Producer Metrics ===\n")
	fmt.Printf("Attempted: %d\n", totalMessages)
	fmt.Printf("Successful: %d\n", successCount.Load())
	fmt.Printf("Failed: %d\n", failureCount.Load())
	fmt.Printf("Throughput: %.2f msg/s\n", float64(successCount.Load())/elapsed.Seconds())
	fmt.Printf("Throughput: %.2f MB/s\n",
		(float64(successCount.Load()*messageSize) / 1024 / 1024 / elapsed.Seconds()))
}
