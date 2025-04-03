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
	"golang.org/x/time/rate"
)

func main() {
	var messageSize int64
	var messageRate int
	var runDuration time.Duration

	fmt.Print("Enter message size (bytes): ")
	fmt.Scan(&messageSize)
	fmt.Print("Enter message rate (messages/sec): ")
	fmt.Scan(&messageRate)
	fmt.Print("Enter run duration (e.g., 5m for 5 minutes): ")
	var durationInput string
	fmt.Scan(&durationInput)

	// Parse the duration input (e.g., "5m" for 5 minutes)
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

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "test_topic",
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
		successCount atomic.Int64
		failureCount atomic.Int64
		limiter      = rate.NewLimiter(rate.Limit(messageRate), messageRate)
		workers      = runtime.NumCPU() * 2
		shutdown     = make(chan struct{})
		start        = time.Now()
		endTime      = start.Add(runDuration) // Calculate end time
	)

	// Signal handling for graceful shutdown
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

					if err := limiter.WaitN(context.Background(), 1); err != nil {
						log.Printf("Rate limiter error: %v", err)
						continue
					}

					_, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
						Payload:   payload,
						EventTime: time.Now(),
					})

					if err != nil {
						failureCount.Add(1)
					} else {
						successCount.Add(1)
					}
				}
			}
		}()
	}

	wg.Wait()
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
}
