package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	warmupCycles    = flag.Int("warmup-cycles", 10000, "Number of warmup cycles to run (same workload as main benchmark)")
	cycles          = flag.Int("cycles", 100000, "Number of workload cycles to run")
	rowCount        = flag.Int64("rows", 1000000, "Number of rows (primary key range) to use for operations")
	goroutines      = flag.Int("goroutines", 64, "Number of concurrent goroutines to run workload cycles")
	seed            = flag.Int64("seed", 12345, "Random seed for deterministic behavior (use same seed for reproducible results)")
	contactPoints   = flag.String("contact-points", "127.0.0.1", "Comma-separated list of Cassandra contact points")
	keyspace        = flag.String("keyspace", "gocqlbenchmarksks", "Keyspace name")
	table           = flag.String("table", "benchmark1", "Table name")
	compression     = flag.Bool("compression", false, "Enable LZ4 compression")
	protoVersion    = flag.Int("proto-version", 4, "Cassandra protocol version")
	metricsInterval = flag.Duration("metrics-interval", time.Second, "Interval for collecting system metrics (e.g., 1s, 500ms)")
	reportFile      = flag.String("report-file", "", "Path to write detailed benchmark report (default: write to stdout)")
	memProfile      = flag.String("memprofile", "", "Path to write memory profile (default: empty string = no profile)")
	cpuProfile      = flag.String("cpuprofile", "", "Path to write cpu profile (default: empty string = no profile)")
)

const (
	letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	// Percentile constants for consistent metrics across the application
	P50  = 0.5   // 50th percentile (median)
	P90  = 0.9   // 90th percentile
	P95  = 0.95  // 95th percentile
	P99  = 0.99  // 99th percentile
	P999 = 0.999 // 99.9th percentile

	// Tolerance for percentile calculations (lower = more precise, higher memory usage)
	PercentileTolerance = 0.0001 // 0.01% tolerance (ultra precise)
)

func main() {
	flag.Parse()

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		err = pprof.StartCPUProfile(f)
		if err != nil {
			log.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}

	// Setup Cassandra cluster
	cluster := &ClusterConfig{
		Hosts:            strings.Split(*contactPoints, ","),
		Compression:      *compression,
		DefaultTimestamp: true,
		ProtoVersion:     *protoVersion,
		Timeout:          30 * time.Second,
	}

	session, err := getSession(*cluster)
	if err != nil {
		log.Fatalf("Failed to create session: %v", err)
	}
	defer session.Close()

	// Create keyspace and table
	if err := setupSchema(session); err != nil {
		log.Fatalf("Failed to setup schema: %v", err)
	}

	// Initialize performance metrics
	metrics := NewPerformanceMetrics()

	// Run warmup
	fmt.Printf("Running warmup with %d cycles...\n", *warmupCycles)
	if err := runWarmup(session); err != nil {
		log.Fatalf("Warmup failed: %v", err)
	}

	// Start system metrics collection after warmup
	fmt.Printf("Starting system metrics collection (interval: %v)...\n", *metricsInterval)
	metrics.StartSystemMetricsCollection(*metricsInterval)
	defer metrics.StopSystemMetricsCollection()

	// Run workload cycles with metrics tracking
	fmt.Printf("Running %d workload cycles with %d goroutines...\n", *cycles, *goroutines)
	metrics.StartTime = time.Now()

	if err := runConcurrentWorkload(session, metrics); err != nil {
		log.Fatalf("Concurrent workload failed: %v", err)
	}

	metrics.EndTime = time.Now()
	totalTime := metrics.EndTime.Sub(metrics.StartTime)
	fmt.Printf("Completed %d cycles in %v\n", *cycles, totalTime)
	fmt.Printf("Average time per cycle: %v\n", totalTime/time.Duration(*cycles))

	session.Close()

	if *memProfile != "" {
		f, _ := os.Create(*memProfile)
		defer f.Close()
		runtime.GC()
		if err := pprof.Lookup("allocs").WriteTo(f, 0); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}

	// Generate detailed report
	if *reportFile != "" {
		// Write to file if report-file is specified
		fmt.Printf("Writing detailed report to: %s\n", *reportFile)
		if err := metrics.WriteDetailedReport(*reportFile); err != nil {
			log.Printf("Warning: Failed to write detailed report: %v", err)
		} else {
			fmt.Printf("Detailed report saved to: %s\n", *reportFile)
		}

		// Print concise summary to stdout
		metrics.PrintConciseSummary()
	} else {
		// Write detailed report to stdout if no file specified
		fmt.Println("Writing detailed report to stdout...")
		metrics.writeDetailedMetricsToWriter(os.Stdout)
	}
}

func setupSchema(session Session) error {
	// Create keyspace
	createKeyspace := fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1}", *keyspace)
	if err := session.Exec(createKeyspace); err != nil {
		return fmt.Errorf("failed to create keyspace: %w", err)
	}

	// Create table
	createTable := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (id bigint PRIMARY KEY, c2 text, c3 bigint, c4 text, c5 text, c6 text, c7 text, c8 bigint)", *keyspace, *table)
	if err := session.Exec(createTable); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Truncate table to start fresh
	truncateTable := fmt.Sprintf("TRUNCATE %s.%s", *keyspace, *table)
	if err := session.Exec(truncateTable); err != nil {
		return fmt.Errorf("failed to truncate table: %w", err)
	}

	time.Sleep(1 * time.Second)

	if err := session.Exec(truncateTable); err != nil {
		return fmt.Errorf("failed to truncate table: %w", err)
	}

	time.Sleep(5 * time.Second)

	return nil
}

func runWarmup(session Session) error {
	// Use nil metrics for warmup (we don't track warmup performance)
	return runConcurrentCycles(session, *warmupCycles, "Warmup", true, nil)
}

func runConcurrentWorkload(session Session, metrics *PerformanceMetrics) error {
	return runConcurrentCycles(session, *cycles, "Worker", false, metrics)
}

func runConcurrentCycles(session Session, totalCycles int, workerPrefix string, isWarmup bool, metrics *PerformanceMetrics) error {
	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Atomic counter for cycle numbers (starts at 0, first cycle will be 1)
	var cycleCounter int64 = 0

	// Progress reporting setup
	startTime := time.Now()
	var lastProgressReport int64 = 0

	// Calculate progress reporting interval (report every 10% or at least every 10 cycles, max every 1 cycle for small runs)
	progressInterval := int64(totalCycles / 10) // Every 10%
	if progressInterval < 10 {
		progressInterval = 10 // At least every 10 cycles
	}
	if progressInterval > int64(totalCycles) {
		progressInterval = 1 // For very small runs, report every cycle
	}
	if totalCycles <= 10 {
		progressInterval = 1 // For runs of 10 or fewer cycles, report each one
	}

	// Create error channel to collect errors from workers
	errorChannel := make(chan error, 1) // Buffer of 1 to prevent blocking on first error

	// Use WaitGroup to wait for all goroutines to complete
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < *goroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for ctx.Err() == nil {
				// Use CAS to atomically get the next cycle number
				cycle := atomic.AddInt64(&cycleCounter, 1)

				// Check if we've exceeded the total number of cycles
				if cycle > int64(totalCycles) {
					return
				}

				// Check if we should report progress
				if cycle%progressInterval == 0 || cycle == 1 || cycle == int64(totalCycles) {
					// Only let one goroutine report progress to avoid duplicate messages
					currentLastReport := atomic.LoadInt64(&lastProgressReport)
					if cycle > currentLastReport && atomic.CompareAndSwapInt64(&lastProgressReport, currentLastReport, cycle) {
						elapsed := time.Since(startTime)
						remaining := int64(totalCycles) - cycle
						var eta time.Duration
						if cycle > 1 {
							avgTimePerCycle := elapsed / time.Duration(cycle)
							eta = avgTimePerCycle * time.Duration(remaining)
						}

						if eta > 0 && remaining > 0 {
							fmt.Printf("[%s] %s progress: %d/%d cycles completed (%.1f%%), %d remaining, ETA: %v\n",
								time.Now().Format("15:04:05"), workerPrefix, cycle, totalCycles,
								float64(cycle)/float64(totalCycles)*100, remaining, eta.Round(time.Second))
						} else {
							fmt.Printf("[%s] %s progress: %d/%d cycles completed (%.1f%%)\n",
								time.Now().Format("15:04:05"), workerPrefix, cycle, totalCycles,
								float64(cycle)/float64(totalCycles)*100)
						}
					}
				}

				// Create RNG with deterministic seed based on cycle and warmup mode
				var rngSeed int64
				if isWarmup {
					// Warmup uses negative offset to ensure different patterns than main workload
					rngSeed = *seed - int64(cycle)
				} else {
					// Main workload uses positive offset
					rngSeed = *seed + int64(cycle)
				}
				rng := rand.New(rand.NewSource(rngSeed))

				// Calculate baseID for this cycle
				baseID := cycle % *rowCount

				// Track cycle timing
				cycleStart := time.Now()
				if err := runWorkloadCycleWithContext(ctx, session, rng, int(cycle), baseID, metrics); err != nil {
					// Track error in metrics if available
					if metrics != nil {
						metrics.RecordError()
					}
					// Send error and cancel context to stop all other goroutines
					select {
					case errorChannel <- fmt.Errorf("%s %d cycle %d failed: %w", strings.ToLower(workerPrefix), workerID, cycle, err):
						cancel() // Cancel context to stop all other workers
					default:
						// Error channel is full, another error already occurred
					}
					return
				}

				// Track successful cycle timing
				if metrics != nil {
					metrics.RecordCycle(time.Since(cycleStart))
				}
			}
		}(i)
	}

	// Wait for all workers to complete or first error
	go func() {
		wg.Wait()
		close(errorChannel)
	}()

	// Return the first error that occurs
	err := <-errorChannel
	cancel() // Ensure context is cancelled
	return err
}

func runWorkloadCycleWithContext(ctx context.Context, session Session, rng *rand.Rand, cycle int, baseID int64, metrics *PerformanceMetrics) error {

	// Check if context is cancelled before starting
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// 1. INSERT operation
	insertQuery := fmt.Sprintf("INSERT INTO %s.%s (id, c2, c3, c4, c5, c6, c7, c8) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", *keyspace, *table)
	insertData := randStringBytes(rng, 16)
	insertc4 := randStringBytes(rng, 32)
	insertc5 := randStringBytes(rng, 64)
	insertc6 := randStringBytes(rng, 128)
	insertc7 := randStringBytes(rng, 1024)
	insertValue := baseID * baseID
	insertc8 := baseID * baseID * baseID

	// Time the INSERT operation
	insertStart := time.Now()
	if err := session.Exec(insertQuery, baseID, insertData, insertValue, insertc4, insertc5, insertc6, insertc7, insertc8); err != nil {
		return fmt.Errorf("INSERT failed: %w", err)
	}
	if metrics != nil {
		metrics.RecordInsert(time.Since(insertStart))
	}

	// Check if context is cancelled before continuing
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// 2. SELECT operation - read the data we just inserted to ensure it exists
	selectQuery := fmt.Sprintf("SELECT * FROM %s.%s WHERE id = ?", *keyspace, *table)

	// Time the SELECT operation
	selectStart := time.Now()

	var id int64
	var c2 string
	var c3 int64
	var c4 string
	var c5 string
	var c6 string
	var c7 string
	var c8 int64

	err := session.Query(selectQuery, []interface{}{baseID}, &id, &c2, &c3, &c4, &c5, &c6, &c7, &c8)
	if err != nil {
		return fmt.Errorf("SELECT failed: %w", err)
	}
	if metrics != nil {
		metrics.RecordSelect(time.Since(selectStart))
	}

	// Verify all columns match what we inserted
	if id != baseID {
		return fmt.Errorf("SELECT failed: expected id %d, got %d", baseID, id)
	}
	if c2 != insertData {
		return fmt.Errorf("SELECT failed: expected c2 %s, got %s", insertData, c2)
	}
	if c3 != insertValue {
		return fmt.Errorf("SELECT failed: expected c3 %d, got %d", insertValue, c3)
	}
	if c4 != insertc4 {
		return fmt.Errorf("SELECT failed: expected c4 %s, got %s", insertc4, c4)
	}
	if c5 != insertc5 {
		return fmt.Errorf("SELECT failed: expected c5 %s, got %s", insertc5, c5)
	}
	if c6 != insertc6 {
		return fmt.Errorf("SELECT failed: expected c6 %s, got %s", insertc6, c6)
	}
	if c7 != insertc7 {
		return fmt.Errorf("SELECT failed: expected c7 %s, got %s", insertc7, c7)
	}
	if c8 != insertc8 {
		return fmt.Errorf("SELECT failed: expected c8 %d, got %d", insertc8, c8)
	}

	return nil
}

func randStringBytes(rng *rand.Rand, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rng.Intn(len(letterBytes))]
	}
	return string(b)
}
