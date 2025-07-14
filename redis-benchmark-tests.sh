#!/bin/bash
echo "=== BitDB Redis Protocol Benchmark Test Suite ==="
echo "Make sure BitDB Redis server is running on port 6379"
echo "Start server with: go run ./cmd/redis-server"
echo ""

# Basic connectivity test
echo "1. Testing basic connectivity..."
redis-cli -p 6379 ping
echo ""

# Basic CRUD operations
echo "2. Testing basic CRUD operations..."
redis-cli -p 6379 set test_key "hello world"
redis-cli -p 6379 get test_key
redis-cli -p 6379 exists test_key
redis-cli -p 6379 del test_key
redis-cli -p 6379 get test_key
echo ""

# SET operations benchmarks
echo "3. SET operation benchmarks..."
echo "Medium dataset (50K operations):"
redis-benchmark -h localhost -p 6379 -t set -n 50000 -c 10 -q

echo "Large dataset (100K operations):"
redis-benchmark -h localhost -p 6379 -t set -n 100000 -c 20 -q

echo "Extra large dataset (500K operations):"
redis-benchmark -h localhost -p 6379 -t set -n 500000 -c 50 -q
echo ""

# GET operations benchmarks
echo "4. GET operation benchmarks..."
echo "Medium dataset (50K operations):"
redis-benchmark -h localhost -p 6379 -t get -n 50000 -c 10 -q

echo "Large dataset (100K operations):"
redis-benchmark -h localhost -p 6379 -t get -n 100000 -c 20 -q

echo "Extra large dataset (500K operations):"
redis-benchmark -h localhost -p 6379 -t get -n 500000 -c 50 -q
echo ""

# Concurrency stress tests
echo "5. Concurrency stress tests..."
echo "High concurrency SET (50 clients):"
redis-benchmark -h localhost -p 6379 -t set -n 100000 -c 50 -q

echo "High concurrency GET (50 clients):"
redis-benchmark -h localhost -p 6379 -t get -n 100000 -c 50 -q

echo "Extreme concurrency SET (100 clients):"
redis-benchmark -h localhost -p 6379 -t set -n 100000 -c 100 -q

echo "Extreme concurrency GET (100 clients):"
redis-benchmark -h localhost -p 6379 -t get -n 100000 -c 100 -q
echo ""

# Large value tests
echo "6. Large value tests..."
echo "1KB values SET:"
redis-benchmark -h localhost -p 6379 -t set -n 25000 -c 20 -d 1024 -q

echo "1KB values GET:"
redis-benchmark -h localhost -p 6379 -t get -n 25000 -c 20 -d 1024 -q

echo "10KB values SET:"
redis-benchmark -h localhost -p 6379 -t set -n 10000 -c 10 -d 10240 -q

echo "10KB values GET:"
redis-benchmark -h localhost -p 6379 -t get -n 10000 -c 10 -d 10240 -q
echo ""
