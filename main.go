package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// SystemMetrics holds system resource usage statistics
type SystemMetrics struct {
	MemoryUsedBytes uint64
	Timestamp       time.Time
}

// PerformanceMetrics tracks detailed performance statistics using Prometheus summaries
type PerformanceMetrics struct {
	// Summary metrics for precise latency percentiles
	InsertLatency prometheus.Summary
	SelectLatency prometheus.Summary
	CycleLatency  prometheus.Summary

	// Counter metrics for operations and errors
	InsertCount prometheus.Counter
	SelectCount prometheus.Counter
	ErrorCount  prometheus.Counter
	CycleCount  prometheus.Counter

	// System resource metrics (gauges for current values)
	MemoryUsedGauge prometheus.Gauge

	// System metrics tracking
	SystemMetrics      []SystemMetrics
	SystemMetricsMutex sync.RWMutex
	StopSystemMetrics  chan struct{}

	// Overall timing
	StartTime time.Time
	EndTime   time.Time

	// Prometheus registry
	Registry *prometheus.Registry
}

// NewPerformanceMetrics creates a new metrics instance with Prometheus summaries for precise percentiles
func NewPerformanceMetrics() *PerformanceMetrics {
	registry := prometheus.NewRegistry()

	// Define high-resolution percentiles with maximum precision tolerances
	// Lower tolerance = higher precision but more memory usage
	objectives := map[float64]float64{
		P50:  PercentileTolerance, // 50th percentile (median)
		P90:  PercentileTolerance, // 90th percentile
		P95:  PercentileTolerance, // 95th percentile
		P99:  PercentileTolerance, // 99th percentile
		P999: PercentileTolerance, // 99.9th percentile
	}

	// Calculate appropriate MaxAge to ensure observations stay relevant for entire benchmark
	// Estimate maximum benchmark duration based on parameters
	totalCycles := *warmupCycles + *cycles
	estimatedCycleTime := time.Second * 1 // Conservative estimate: 1 second per cycle
	estimatedDuration := time.Duration(totalCycles) * estimatedCycleTime

	// Add buffer for safety (2x the estimated duration, minimum 30 minutes, maximum 24 hours)
	maxAge := estimatedDuration * 2
	if maxAge < time.Minute*30 {
		maxAge = time.Minute * 30 // Minimum 30 minutes
	}
	if maxAge > time.Hour*24 {
		maxAge = time.Hour * 24 // Maximum 24 hours to prevent excessive memory usage
	}

	// Helper function to create summary metrics with consistent configuration
	createSummary := func(name, help string) prometheus.Summary {
		return prometheus.NewSummary(prometheus.SummaryOpts{
			Name:       name,
			Help:       help,
			Objectives: objectives,
			MaxAge:     maxAge,
		})
	}

	// Create summary metrics for precise percentile calculations
	insertLatency := createSummary("cassandra_insert_duration_seconds", "Summary of INSERT operation latencies with precise percentiles")
	selectLatency := createSummary("cassandra_select_duration_seconds", "Summary of SELECT operation latencies with precise percentiles")
	cycleLatency := createSummary("cassandra_cycle_duration_seconds", "Summary of complete cycle latencies with precise percentiles")

	// Helper function to create counter metrics with consistent configuration
	createCounter := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{
			Name: name,
			Help: help,
		})
	}

	// Create counter metrics
	insertCount := createCounter("cassandra_insert_operations_total", "Total number of INSERT operations")
	selectCount := createCounter("cassandra_select_operations_total", "Total number of SELECT operations")
	errorCount := createCounter("cassandra_error_operations_total", "Total number of failed operations")
	cycleCount := createCounter("cassandra_cycle_operations_total", "Total number of completed cycles")

	// Helper function to create gauge metrics with consistent configuration
	createGauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{
			Name: name,
			Help: help,
		})
	}

	// Create system resource gauges
	memoryUsedGauge := createGauge("system_memory_used_bytes", "Current memory usage in bytes")

	// Register metrics
	registry.MustRegister(insertLatency, selectLatency, cycleLatency)
	registry.MustRegister(insertCount, selectCount, errorCount, cycleCount)
	registry.MustRegister(memoryUsedGauge)

	// Log the calculated MaxAge for transparency
	fmt.Printf("Summary metrics configured with MaxAge: %v (ensures all observations stay relevant)\n", maxAge)

	return &PerformanceMetrics{
		InsertLatency:     insertLatency,
		SelectLatency:     selectLatency,
		CycleLatency:      cycleLatency,
		InsertCount:       insertCount,
		SelectCount:       selectCount,
		ErrorCount:        errorCount,
		CycleCount:        cycleCount,
		MemoryUsedGauge:   memoryUsedGauge,
		SystemMetrics:     make([]SystemMetrics, 0),
		StopSystemMetrics: make(chan struct{}),
		Registry:          registry,
	}
}

// RecordInsert records an INSERT operation timing
func (m *PerformanceMetrics) RecordInsert(duration time.Duration) {
	m.InsertLatency.Observe(duration.Seconds())
	m.InsertCount.Inc()
}

// RecordSelect records a SELECT operation timing
func (m *PerformanceMetrics) RecordSelect(duration time.Duration) {
	m.SelectLatency.Observe(duration.Seconds())
	m.SelectCount.Inc()
}

// RecordCycle records a complete cycle timing
func (m *PerformanceMetrics) RecordCycle(duration time.Duration) {
	m.CycleLatency.Observe(duration.Seconds())
	m.CycleCount.Inc()
}

// RecordError records an error occurrence
func (m *PerformanceMetrics) RecordError() {
	m.ErrorCount.Inc()
}

// collectSystemMetrics gathers current system resource usage
func (m *PerformanceMetrics) collectSystemMetrics() SystemMetrics {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Get Go runtime memory info
	// memStats.Alloc = bytes allocated and still in use (heap)
	usedMemory := memStats.Alloc

	return SystemMetrics{
		MemoryUsedBytes: usedMemory,
		Timestamp:       time.Now(),
	}
}

// updatePrometheusGauges updates the Prometheus gauge metrics with current system stats
func (m *PerformanceMetrics) updatePrometheusGauges(stats SystemMetrics) {
	m.MemoryUsedGauge.Set(float64(stats.MemoryUsedBytes))
}

// StartSystemMetricsCollection begins periodic collection of system metrics
func (m *PerformanceMetrics) StartSystemMetricsCollection(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				stats := m.collectSystemMetrics()
				m.updatePrometheusGauges(stats)

				// Store historical data
				m.SystemMetricsMutex.Lock()
				m.SystemMetrics = append(m.SystemMetrics, stats)
				m.SystemMetricsMutex.Unlock()

			case <-m.StopSystemMetrics:
				return
			}
		}
	}()
}

// StopSystemMetricsCollection stops the periodic collection of system metrics
func (m *PerformanceMetrics) StopSystemMetricsCollection() {
	close(m.StopSystemMetrics)
}

// generateReportFilename creates a default report filename with timestamp
func generateReportFilename() string {
	timestamp := time.Now().Format("20060102_150405")
	return fmt.Sprintf("benchmark_report_%s.txt", timestamp)
}

// WriteDetailedReport writes a comprehensive report to a file
func (m *PerformanceMetrics) WriteDetailedReport(filename string) error {
	if filename == "" {
		filename = generateReportFilename()
	}

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create report file: %w", err)
	}
	defer file.Close()

	// Write report header
	fmt.Fprintf(file, "GOCQL BENCHMARK DETAILED REPORT\n")
	fmt.Fprintf(file, "Generated: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Fprintf(file, "%s\n\n", strings.Repeat("=", 80))

	// Write all the detailed metrics to file
	m.writeDetailedMetricsToWriter(file)

	return nil
}

// writeDetailedMetricsToWriter writes all metrics to the provided writer
func (m *PerformanceMetrics) writeDetailedMetricsToWriter(w io.Writer) {
	totalDuration := m.EndTime.Sub(m.StartTime)

	fmt.Fprintf(w, "PERFORMANCE SUMMARY WITH PERCENTILES\n")
	fmt.Fprintf(w, "%s\n", strings.Repeat("=", 80))

	// Overall statistics
	fmt.Fprintf(w, "Total Runtime: %v\n", totalDuration)

	// Get stats for each operation type
	insertStats := m.GetSummaryStats(m.InsertLatency)
	selectStats := m.GetSummaryStats(m.SelectLatency)
	cycleStats := m.GetSummaryStats(m.CycleLatency)

	if cycleCount := cycleStats["count"]; cycleCount > 0 {
		fmt.Fprintf(w, "Total Cycles: %.0f\n", cycleCount)
		fmt.Fprintf(w, "Average Cycle Time: %.3fms (%.0fμs)\n", cycleStats["avg"]*1000, cycleStats["avg"]*1000000)
		fmt.Fprintf(w, "Cycles per Second: %.3f\n", cycleCount/totalDuration.Seconds())
	}

	// Get error count using gatherer
	errorRegistry := prometheus.NewRegistry()
	errorRegistry.MustRegister(m.ErrorCount)
	errorFamilies, err := errorRegistry.Gather()
	errorCount := float64(0)
	if err == nil && len(errorFamilies) > 0 && len(errorFamilies[0].Metric) > 0 {
		errorCount = errorFamilies[0].Metric[0].Counter.GetValue()
	}
	fmt.Fprintf(w, "Total Errors: %.0f\n", errorCount)

	fmt.Fprintf(w, "\n")

	// Operation statistics with percentiles
	m.writeOperationStatsWithPercentilesToWriter(w, "INSERT", insertStats)
	m.writeOperationStatsWithPercentilesToWriter(w, "SELECT", selectStats)
	m.writeOperationStatsWithPercentilesToWriter(w, "CYCLE", cycleStats)

	// Throughput statistics
	totalOps := insertStats["count"] + selectStats["count"]
	if totalOps > 0 && totalDuration.Seconds() > 0 {
		fmt.Fprintf(w, "%s\n", strings.Repeat("-", 80))
		fmt.Fprintf(w, "Total Operations: %.0f\n", totalOps)
		fmt.Fprintf(w, "Operations per Second: %.3f\n", totalOps/totalDuration.Seconds())

		if insertCount := insertStats["count"]; insertCount > 0 {
			fmt.Fprintf(w, "Inserts per Second: %.3f\n", insertCount/totalDuration.Seconds())
		}
		if selectCount := selectStats["count"]; selectCount > 0 {
			fmt.Fprintf(w, "Selects per Second: %.3f\n", selectCount/totalDuration.Seconds())
		}
	}

	// System metrics summary
	m.writeSystemMetricsSummaryToWriter(w)

	fmt.Fprintf(w, "%s\n", strings.Repeat("=", 80))
}

// writeOperationStatsWithPercentiles writes operation stats with percentiles to any writer
func writeOperationStatsWithPercentiles(w io.Writer, opName string, stats map[string]float64) {
	count := stats["count"]
	if count == 0 {
		fmt.Fprintf(w, "%s Operations: 0\n", opName)
		return
	}

	fmt.Fprintf(w, "%s Operations: %.0f\n", opName, count)
	fmt.Fprintf(w, "  Average Time: %.3fms (%.0fμs)\n", stats["avg"]*1000, stats["avg"]*1000000)

	// Print percentiles if available
	if p50, exists := stats["p50"]; exists {
		fmt.Fprintf(w, "  50th Percentile (Median): %.3fms (%.0fμs)\n", p50*1000, p50*1000000)
	}
	if p90, exists := stats["p90"]; exists {
		fmt.Fprintf(w, "  90th Percentile: %.3fms (%.0fμs)\n", p90*1000, p90*1000000)
	}
	if p95, exists := stats["p95"]; exists {
		fmt.Fprintf(w, "  95th Percentile: %.3fms (%.0fμs)\n", p95*1000, p95*1000000)
	}
	if p99, exists := stats["p99"]; exists {
		fmt.Fprintf(w, "  99th Percentile: %.3fms (%.0fμs)\n", p99*1000, p99*1000000)
	}
	if p999, exists := stats["p99.9"]; exists {
		fmt.Fprintf(w, "  99.9th Percentile: %.3fms (%.0fμs)\n", p999*1000, p999*1000000)
	}

	fmt.Fprintf(w, "  Total Time: %.3fms\n", stats["sum"]*1000)
	fmt.Fprintf(w, "\n")
}

// writeOperationStatsWithPercentilesToWriter writes operation stats to a writer
func (m *PerformanceMetrics) writeOperationStatsWithPercentilesToWriter(w io.Writer, opName string, stats map[string]float64) {
	writeOperationStatsWithPercentiles(w, opName, stats)
}

// GetSummaryStats extracts precise percentile and statistical data from a summary
func (m *PerformanceMetrics) GetSummaryStats(summary prometheus.Summary) map[string]float64 {
	// Use prometheus.Gatherer to get metrics data
	registry := prometheus.NewRegistry()
	registry.MustRegister(summary)

	metricFamilies, err := registry.Gather()
	if err != nil || len(metricFamilies) == 0 {
		return map[string]float64{}
	}

	metricFamily := metricFamilies[0]
	if len(metricFamily.Metric) == 0 || metricFamily.Metric[0].Summary == nil {
		return map[string]float64{}
	}

	summaryMetric := metricFamily.Metric[0].Summary

	// Extract basic statistics
	totalCount := float64(summaryMetric.GetSampleCount())

	if totalCount == 0 {
		return map[string]float64{}
	}

	stats := map[string]float64{
		"count": totalCount,
		"sum":   summaryMetric.GetSampleSum(),
		"avg":   summaryMetric.GetSampleSum() / totalCount,
	}

	// Extract precise percentiles from summary quantiles
	quantiles := summaryMetric.GetQuantile()
	for _, quantile := range quantiles {
		percentile := quantile.GetQuantile() * 100
		value := quantile.GetValue()
		stats[fmt.Sprintf("p%.0f", percentile)] = value
	}

	return stats
}

// PrintConciseSummary prints a brief summary to stdout
func (m *PerformanceMetrics) PrintConciseSummary() {
	totalDuration := m.EndTime.Sub(m.StartTime)

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("BENCHMARK SUMMARY")
	fmt.Println(strings.Repeat("=", 60))

	// Overall statistics
	fmt.Printf("Total Runtime: %v\n", totalDuration)

	// Get stats for each operation type
	insertStats := m.GetSummaryStats(m.InsertLatency)
	selectStats := m.GetSummaryStats(m.SelectLatency)
	cycleStats := m.GetSummaryStats(m.CycleLatency)

	if cycleCount := cycleStats["count"]; cycleCount > 0 {
		fmt.Printf("Total Cycles: %.0f\n", cycleCount)
		fmt.Printf("Cycles per Second: %.3f\n", cycleCount/totalDuration.Seconds())
	}

	// Get error count
	errorRegistry := prometheus.NewRegistry()
	errorRegistry.MustRegister(m.ErrorCount)
	errorFamilies, err := errorRegistry.Gather()
	errorCount := float64(0)
	if err == nil && len(errorFamilies) > 0 && len(errorFamilies[0].Metric) > 0 {
		errorCount = errorFamilies[0].Metric[0].Counter.GetValue()
	}
	fmt.Printf("Total Errors: %.0f\n", errorCount)

	// Throughput statistics
	totalOps := insertStats["count"] + selectStats["count"]
	if totalOps > 0 && totalDuration.Seconds() > 0 {
		fmt.Printf("Operations per Second: %.3f\n", totalOps/totalDuration.Seconds())
	}

	// Median latencies (50th percentile)
	if insertStats["count"] > 0 {
		if p50, exists := insertStats["p50"]; exists {
			fmt.Printf("Median INSERT Latency: %.3fms (%.0fμs)\n", p50*1000, p50*1000000)
		} else {
			fmt.Printf("Median INSERT Latency: %.3fms (%.0fμs)\n", insertStats["avg"]*1000, insertStats["avg"]*1000000)
		}
	}
	if selectStats["count"] > 0 {
		if p50, exists := selectStats["p50"]; exists {
			fmt.Printf("Median SELECT Latency: %.3fms (%.0fμs)\n", p50*1000, p50*1000000)
		} else {
			fmt.Printf("Median SELECT Latency: %.3fms (%.0fμs)\n", selectStats["avg"]*1000, selectStats["avg"]*1000000)
		}
	}

	// System metrics summary (brief)
	m.SystemMetricsMutex.RLock()
	if len(m.SystemMetrics) > 0 {
		var totalMemUsed uint64
		count := len(m.SystemMetrics)

		for _, stats := range m.SystemMetrics {
			totalMemUsed += stats.MemoryUsedBytes
		}

		avgMemMB := float64(totalMemUsed/uint64(count)) / 1024 / 1024

		fmt.Printf("Average Memory Usage: %.3f MB\n", avgMemMB)
	}
	m.SystemMetricsMutex.RUnlock()

	fmt.Println(strings.Repeat("=", 60))
}

// PrintSummary prints a comprehensive performance summary with percentiles
func (m *PerformanceMetrics) PrintSummary() {
	totalDuration := m.EndTime.Sub(m.StartTime)

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("PERFORMANCE SUMMARY WITH PERCENTILES")
	fmt.Println(strings.Repeat("=", 80))

	// Overall statistics
	fmt.Printf("Total Runtime: %v\n", totalDuration)

	// Get stats for each operation type
	insertStats := m.GetSummaryStats(m.InsertLatency)
	selectStats := m.GetSummaryStats(m.SelectLatency)
	cycleStats := m.GetSummaryStats(m.CycleLatency)

	if cycleCount := cycleStats["count"]; cycleCount > 0 {
		fmt.Printf("Total Cycles: %.0f\n", cycleCount)
		fmt.Printf("Average Cycle Time: %.3fms (%.0fμs)\n", cycleStats["avg"]*1000, cycleStats["avg"]*1000000)
		fmt.Printf("Cycles per Second: %.3f\n", cycleCount/totalDuration.Seconds())
	}

	// Get error count using gatherer
	errorRegistry := prometheus.NewRegistry()
	errorRegistry.MustRegister(m.ErrorCount)
	errorFamilies, err := errorRegistry.Gather()
	errorCount := float64(0)
	if err == nil && len(errorFamilies) > 0 && len(errorFamilies[0].Metric) > 0 {
		errorCount = errorFamilies[0].Metric[0].Counter.GetValue()
	}
	fmt.Printf("Total Errors: %.0f\n", errorCount)

	fmt.Println()

	// Operation statistics with percentiles
	m.printOperationStatsWithPercentiles("INSERT", insertStats)
	m.printOperationStatsWithPercentiles("SELECT", selectStats)
	m.printOperationStatsWithPercentiles("CYCLE", cycleStats)

	// Throughput statistics
	totalOps := insertStats["count"] + selectStats["count"]
	if totalOps > 0 && totalDuration.Seconds() > 0 {
		fmt.Println(strings.Repeat("-", 80))
		fmt.Printf("Total Operations: %.0f\n", totalOps)
		fmt.Printf("Operations per Second: %.3f\n", totalOps/totalDuration.Seconds())

		if insertCount := insertStats["count"]; insertCount > 0 {
			fmt.Printf("Inserts per Second: %.3f\n", insertCount/totalDuration.Seconds())
		}
		if selectCount := selectStats["count"]; selectCount > 0 {
			fmt.Printf("Selects per Second: %.3f\n", selectCount/totalDuration.Seconds())
		}
	}

	// System metrics summary
	m.printSystemMetricsSummary()

	fmt.Println(strings.Repeat("=", 80))
}

// SystemMetricsStats holds calculated statistics for system metrics
type SystemMetricsStats struct {
	Memory struct {
		AverageUsedMB, MinUsedMB, MaxUsedMB float64
	}
	SampleCount int
}

// calculateSystemMetricsStats computes statistics from collected system metrics
func (m *PerformanceMetrics) calculateSystemMetricsStats() *SystemMetricsStats {
	m.SystemMetricsMutex.RLock()
	defer m.SystemMetricsMutex.RUnlock()

	if len(m.SystemMetrics) == 0 {
		return nil
	}

	stats := &SystemMetricsStats{
		SampleCount: len(m.SystemMetrics),
	}

	// Initialize with first sample
	first := m.SystemMetrics[0]
	stats.Memory.MinUsedMB = float64(first.MemoryUsedBytes) / 1024 / 1024
	stats.Memory.MaxUsedMB = stats.Memory.MinUsedMB

	// Calculate totals and find min/max
	var totalMemUsed uint64

	for _, metric := range m.SystemMetrics {
		totalMemUsed += metric.MemoryUsedBytes

		// Update min/max values
		memUsedMB := float64(metric.MemoryUsedBytes) / 1024 / 1024
		if memUsedMB < stats.Memory.MinUsedMB {
			stats.Memory.MinUsedMB = memUsedMB
		}
		if memUsedMB > stats.Memory.MaxUsedMB {
			stats.Memory.MaxUsedMB = memUsedMB
		}
	}

	count := float64(len(m.SystemMetrics))
	stats.Memory.AverageUsedMB = float64(totalMemUsed/uint64(count)) / 1024 / 1024

	return stats
}

// writeSystemMetricsSummaryToWriter writes system metrics summary to a writer
func (m *PerformanceMetrics) writeSystemMetricsSummaryToWriter(w io.Writer) {
	stats := m.calculateSystemMetricsStats()
	if stats == nil {
		fmt.Fprintf(w, "No system metrics collected\n")
		return
	}

	fmt.Fprintf(w, "%s\n", strings.Repeat("-", 80))
	fmt.Fprintf(w, "SYSTEM RESOURCE USAGE SUMMARY\n")
	fmt.Fprintf(w, "%s\n", strings.Repeat("-", 80))

	// Print Memory statistics
	fmt.Fprintf(w, "Memory Usage (Go heap allocation):\n")
	fmt.Fprintf(w, "  Average Used: %.3f MB\n", stats.Memory.AverageUsedMB)
	fmt.Fprintf(w, "  Min Used: %.3f MB\n", stats.Memory.MinUsedMB)
	fmt.Fprintf(w, "  Max Used: %.3f MB\n", stats.Memory.MaxUsedMB)
	fmt.Fprintf(w, "\n")

	fmt.Fprintf(w, "System metrics collected over %d samples\n", stats.SampleCount)
}

// printSystemMetricsSummary prints a summary of system resource usage during the benchmark
func (m *PerformanceMetrics) printSystemMetricsSummary() {
	stats := m.calculateSystemMetricsStats()
	if stats == nil {
		fmt.Println("No system metrics collected")
		return
	}

	fmt.Println(strings.Repeat("-", 80))
	fmt.Println("SYSTEM RESOURCE USAGE SUMMARY")
	fmt.Println(strings.Repeat("-", 80))

	// Print Memory statistics
	fmt.Printf("Memory Usage (Go heap allocation):\n")
	fmt.Printf("  Average Used: %.3f MB\n", stats.Memory.AverageUsedMB)
	fmt.Printf("  Min Used: %.3f MB\n", stats.Memory.MinUsedMB)
	fmt.Printf("  Max Used: %.3f MB\n", stats.Memory.MaxUsedMB)
	fmt.Println()

	fmt.Printf("System metrics collected over %d samples\n", stats.SampleCount)
}

// printOperationStatsWithPercentiles prints operation stats to stdout
func (m *PerformanceMetrics) printOperationStatsWithPercentiles(opName string, stats map[string]float64) {
	writeOperationStatsWithPercentiles(os.Stdout, opName, stats)
}

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
		pprof.WriteHeapProfile(f)
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
	createTable := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (id bigint PRIMARY KEY, c2 text, c3 bigint)", *keyspace, *table)
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
	insertQuery := fmt.Sprintf("INSERT INTO %s.%s (id, c2, c3) VALUES (?, ?, ?)", *keyspace, *table)
	insertData := randStringBytes(rng, 16)
	insertValue := baseID * baseID

	// Time the INSERT operation
	insertStart := time.Now()
	if err := session.Exec(insertQuery, baseID, insertData, insertValue); err != nil {
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

	err := session.Query(selectQuery, []interface{}{baseID}, &id, &c2, &c3)
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

	return nil
}

func randStringBytes(rng *rand.Rand, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rng.Intn(len(letterBytes))]
	}
	return string(b)
}
