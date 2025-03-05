package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	pulsar "github.com/apache/pulsar-client-go/pulsar"
)

type Metrics struct {
	count    atomic.Int64
	bytes    atomic.Int64
	min      atomic.Int64
	max      atomic.Int64
	sum      atomic.Int64
	windowed struct {
		count atomic.Int64
		sum   atomic.Int64
	}
}

func main() {
	var batchSize int
	fmt.Print("Enter batch size: ")
	fmt.Scan(&batchSize)

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

	var (
		metrics     Metrics
		ctx, cancel = context.WithCancel(context.Background())
		sigchan     = make(chan os.Signal, 1)
		start       = time.Now()
	)

	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Start metric reporter
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				windowCount := metrics.windowed.count.Swap(0)
				windowSum := metrics.windowed.sum.Swap(0)

				var avg time.Duration
				if windowCount > 0 {
					avg = time.Duration(windowSum / windowCount)
				}

				fmt.Printf("[Window] Messages: %d | Avg Latency: %v\n",
					windowCount,
					avg.Round(time.Microsecond))
			case <-ctx.Done():
				return
			}
		}
	}()

	fmt.Println("Consumer started. Processing messages...")

ConsumerLoop:
	for {
		select {
		case <-sigchan:
			fmt.Println("\nTerminating...")
			break ConsumerLoop
		default:
			msg, err := consumer.Receive(ctx)
			if err != nil {
				log.Printf("Error: %v", err)
				continue
			}

			latency := time.Since(msg.EventTime())
			size := len(msg.Payload())

			// Update metrics
			metrics.count.Add(1)
			metrics.bytes.Add(int64(size))

			latencyNs := latency.Nanoseconds()
			metrics.sum.Add(latencyNs)
			metrics.windowed.sum.Add(latencyNs)
			metrics.windowed.count.Add(1)

			// Update min
			for {
				currentMin := metrics.min.Load()
				if latencyNs < currentMin || currentMin == 0 {
					if metrics.min.CompareAndSwap(currentMin, latencyNs) {
						break
					}
				} else {
					break
				}
			}

			// Update max
			for {
				currentMax := metrics.max.Load()
				if latencyNs > currentMax {
					if metrics.max.CompareAndSwap(currentMax, latencyNs) {
						break
					}
				} else {
					break
				}
			}

			consumer.Ack(msg)

			if metrics.count.Load() >= int64(batchSize) {
				break ConsumerLoop
			}
		}
	}

	cancel()
	elapsed := time.Since(start)
	totalCount := metrics.count.Load()

	fmt.Printf("\n=== Consumer Metrics ===\n")
	fmt.Printf("Messages received: %d\n", totalCount)
	fmt.Printf("Time elapsed: %.2fs\n", elapsed.Seconds())
	fmt.Printf("Throughput: %.2f msg/s\n", float64(totalCount)/elapsed.Seconds())
	fmt.Printf("Throughput: %.2f MB/s\n", float64(metrics.bytes.Load())/1024/1024/elapsed.Seconds())

	if totalCount > 0 {
		fmt.Printf("Latency (min/mean/max): %s / %s / %s\n",
			time.Duration(metrics.min.Load()).Round(time.Microsecond),
			time.Duration(metrics.sum.Load()/totalCount).Round(time.Microsecond),
			time.Duration(metrics.max.Load()).Round(time.Microsecond))
	}
}
