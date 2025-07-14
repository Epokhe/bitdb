// Redis-compatible server for BitDB
//
// This implements a Redis wire protocol server that allows BitDB to be tested
// with standard Redis tools like redis-benchmark and redis-cli.
//
// Protocol Reference: https://redis.io/docs/reference/protocol-spec/
// RESP Documentation: https://redis.io/docs/reference/protocol-spec/#resp-protocol-description
package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/epokhe/bitdb/core"
)

func main() {
	// Initialize BitDB with Redis-optimized settings
	db, err := core.Open("./redis-data",
		core.WithRolloverThreshold(10*1024*1024), // 10MB segments for reasonable file sizes
		core.WithMergeEnabled(true),              // Enable background merging
		core.WithMergeThreshold(10),              // Merge when 10+ inactive segments
	)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start Redis-compatible server on standard Redis port
	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	defer listener.Close()

	log.Println("BitDB Redis-compatible server listening on :6379")

	// Accept connections and handle them concurrently
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}

		// Handle each connection in a separate goroutine
		go handleConnection(conn, db)
	}
}

// handleConnection processes a single client connection using the Redis RESP protocol
//
// RESP (Redis Serialization Protocol) is a simple protocol designed for Redis
// that supports different data types. Commands are sent as arrays of bulk strings.
//
// Example RESP command: SET key value
// *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
//
// Reference: https://redis.io/docs/reference/protocol-spec/#resp-arrays
func handleConnection(conn net.Conn, db *core.DB) {
	defer conn.Close()

	// Use buffered I/O for better performance with small Redis commands
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	defer writer.Flush()

	// Process commands in a loop until client disconnects
	for {
		// Parse incoming RESP command into string arguments
		cmd, err := parseRESP(reader)
		if err != nil {
			if err == io.EOF {
				return // Client disconnected cleanly
			}
			log.Printf("Parse error: %v", err)
			writer.WriteString(writeError("ERR parse error"))
			continue
		}

		// Execute the parsed command against BitDB
		response := executeCommand(db, cmd)

		// Send RESP-formatted response back to client
		_, err = writer.WriteString(response)
		if err != nil {
			log.Printf("Write error: %v", err)
			return // Connection error, client likely disconnected
		}

		// Ensure response is sent immediately
		err = writer.Flush()
		if err != nil {
			log.Printf("Flush error: %v", err)
			return // Flush error, connection broken
		}
	}
}

// parseRESP parses Redis RESP protocol commands into string arrays
//
// RESP Protocol Format (https://redis.io/docs/reference/protocol-spec/):
// - Commands are sent as arrays of bulk strings
// - Arrays start with '*' followed by element count: *3\r\n
// - Bulk strings start with '$' followed by byte length: $3\r\n
// - Then the actual string data followed by \r\n
//
// Example: SET key value becomes:
// *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
//
// This parser converts RESP arrays into Go string slices: ["SET", "key", "value"]
func parseRESP(reader *bufio.Reader) ([]string, error) {
	// Step 1: Read array header line (e.g., "*3\r\n")
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 || line[0] != '*' {
		return nil, errors.New("expected array")
	}

	// Step 2: Parse array length from "*N" format
	length, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %v", err)
	}

	// Step 3: Parse each bulk string element in the array
	args := make([]string, length)
	for i := 0; i < length; i++ {
		// Read bulk string header line (e.g., "$3\r\n")
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		line = strings.TrimRight(line, "\r\n")
		if len(line) == 0 || line[0] != '$' {
			return nil, errors.New("expected bulk string")
		}

		// Parse string length from "$N" format
		strLen, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid string length: %v", err)
		}

		// Handle null strings (Redis protocol allows $-1 for null)
		if strLen == -1 {
			args[i] = "" // Treat null as empty string
			continue
		}

		// Read the actual string data plus \r\n terminator
		data := make([]byte, strLen+2) // +2 for \r\n
		_, err = io.ReadFull(reader, data)
		if err != nil {
			return nil, err
		}

		// Extract just the string content (excluding \r\n)
		args[i] = string(data[:strLen])
	}

	return args, nil
}

// executeCommand executes Redis commands using BitDB and returns RESP-formatted responses
//
// Supported Commands (following Redis command specifications):
// - PING: Test connection, returns "PONG" 
// - SET key value: Store key-value pair, returns "OK"
// - GET key: Retrieve value for key, returns value or null
// - DEL key: Delete key, returns 1 if deleted or 0 if key didn't exist
// - EXISTS key: Check if key exists, returns 1 if exists or 0 if not
//
// Redis command reference: https://redis.io/commands/
func executeCommand(db *core.DB, args []string) string {
	if len(args) == 0 {
		return writeError("ERR empty command")
	}

	// Redis commands are case-insensitive
	cmd := strings.ToUpper(args[0])

	switch cmd {
	case "PING":
		// PING command for connection testing
		// Redis spec: https://redis.io/commands/ping/
		return writeBulkString("PONG")

	case "SET":
		// SET key value - store a key-value pair
		// Redis spec: https://redis.io/commands/set/
		if len(args) != 3 {
			return writeError("ERR wrong number of arguments for 'SET' command")
		}
		key, value := args[1], args[2]

		if err := db.Set(key, value); err != nil {
			return writeError(fmt.Sprintf("ERR %v", err))
		}
		return writeSimpleString("OK") // Redis standard response for successful SET

	case "GET":
		// GET key - retrieve value for a key
		// Redis spec: https://redis.io/commands/get/
		if len(args) != 2 {
			return writeError("ERR wrong number of arguments for 'GET' command")
		}
		key := args[1]

		value, err := db.Get(key)
		if err != nil {
			if errors.Is(err, core.ErrKeyNotFound) {
				return writeNull() // Redis returns null for missing keys
			}
			return writeError(fmt.Sprintf("ERR %v", err))
		}
		return writeBulkString(value)

	case "DEL":
		// DEL key - delete a key
		// Redis spec: https://redis.io/commands/del/
		if len(args) != 2 {
			return writeError("ERR wrong number of arguments for 'DEL' command")
		}
		key := args[1]

		err := db.Delete(key)
		if err != nil {
			if errors.Is(err, core.ErrKeyNotFound) {
				return writeInteger(0) // Redis returns 0 for non-existent keys
			}
			return writeError(fmt.Sprintf("ERR %v", err))
		}
		return writeInteger(1) // Redis returns 1 for successfully deleted keys

	case "EXISTS":
		// EXISTS key - check if key exists  
		// Redis spec: https://redis.io/commands/exists/
		if len(args) != 2 {
			return writeError("ERR wrong number of arguments for 'EXISTS' command")
		}
		key := args[1]

		_, err := db.Get(key)
		if err != nil {
			if errors.Is(err, core.ErrKeyNotFound) {
				return writeInteger(0) // Key doesn't exist
			}
			return writeError(fmt.Sprintf("ERR %v", err))
		}
		return writeInteger(1) // Key exists

	default:
		return writeError(fmt.Sprintf("ERR unknown command '%s'", cmd))
	}
}

// RESP response formatters for Redis protocol compliance
// Reference: https://redis.io/docs/reference/protocol-spec/#resp-protocol-description

// writeSimpleString formats a simple string response in RESP format
// Simple strings start with '+' and end with \r\n (e.g., "+OK\r\n")
// Used for status messages like "OK" responses from SET commands
func writeSimpleString(s string) string {
	return fmt.Sprintf("+%s\r\n", s)
}

// writeBulkString formats a bulk string response in RESP format
// Bulk strings start with '$' followed by length, then \r\n, then data, then \r\n
// Example: "hello" becomes "$5\r\nhello\r\n"
// Used for GET command responses and other string data
func writeBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

// writeInteger formats an integer response in RESP format
// Integers start with ':' followed by the number and \r\n (e.g., ":42\r\n")
// Used for DEL and EXISTS commands that return numeric counts
func writeInteger(i int) string {
	return fmt.Sprintf(":%d\r\n", i)
}

// writeNull formats a null bulk string in RESP format
// Null values are represented as "$-1\r\n" in Redis protocol
// Used when GET command finds no value for the requested key
func writeNull() string {
	return "$-1\r\n"
}

// writeError formats an error response in RESP format
// Errors start with '-' followed by the error message and \r\n
// Example: "ERR something went wrong" becomes "-ERR something went wrong\r\n"
// Used for command syntax errors, database errors, and unknown commands
func writeError(msg string) string {
	return fmt.Sprintf("-%s\r\n", msg)
}
