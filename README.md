# gocql-benchmarks

A Go program that runs CQL workload benchmarks against a Cassandra cluster.

## Features

- Configurable workload cycles with INSERT, UPDATE, and SELECT operations
- Warmup phase to prepare the cluster
- Configurable contact points, keyspace, and table names
- Support for LZ4 compression
- Protocol version configuration
- Performance timing and reporting

## Usage

### Basic Usage
`ash
go run main.go
`

### With Custom Parameters
`ash
go run main.go --contact-points="192.168.1.100,192.168.1.101" --cycles=50 --warmup-count=500
`

### Command Line Options

- --contact-points: Comma-separated list of Cassandra contact points (default: "127.0.0.1")
- --cycles: Number of workload cycles to run (default: 100)
- --warmup-count: Number of iterations during the warmup phase (default: 1000)
- --count: Number of iterations (default: 1000)
- --keyspace: Keyspace name (default: "gocqlbenchmarksks")
- --table: Table name (default: "benchmark2")
- --compression: Enable LZ4 compression
- --proto-version: Cassandra protocol version (default: 4)

## Workload Description

Each workload cycle consists of three operations:

1. **INSERT**: Inserts a new row with random data
2. **UPDATE**: Updates an existing row with new random data
3. **SELECT**: Selects a row by ID

The program automatically:
- Creates the keyspace and table if they don't exist
- Truncates the table at startup for consistent results
- Runs a warmup phase before the main workload
- Reports timing statistics after completion

## Table Schema

The benchmark uses the following table schema:
`sql
CREATE TABLE benchmark2 (
    id bigint PRIMARY KEY,
    c2 text,
    c3 bigint
);
`

## Building

`ash
go build -o gocql-benchmark.exe main.go
`

## Requirements

- Go 1.24+
- Cassandra cluster accessible from the running machine
- gocql library (automatically managed via go.mod)
