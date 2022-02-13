package main

import (
	"bufio"
	"fmt"
	"log"
	"strconv"
	"sync"

	// Uncomment this block to pass the first stage
	"net"
	"os"
)

var responses = make(chan string, 10)
var responseWg sync.WaitGroup
var arrWg sync.WaitGroup

func toRESPString(message string) string {
	return "+" + message + "\r\n"
}

func writeResponse(conn net.Conn) {
	for response := range responses {
		fmt.Println("Response", response)
		conn.Write([]byte(response))
		responseWg.Done()
	}
}

type redisData struct {
	simpleString string
	errorString  []byte
	bulkString   string
	integer      int
	array        []redisData
}

func execRedisData(redisData []redisData) {
	for _, request := range redisData {
		if request.bulkString != "" {
			responses <- toRESPString("PONG")
			responseWg.Add(1)
		}
	}
}

// Expect every message to be in RESP and be an Array of bulk strings
func parseRedisData(reader bufio.Reader) (redisData, error) {
	dataByte, err := reader.ReadByte()
	data := redisData{}
	fmt.Println("Data byte", string(dataByte))

	if err != nil {
		// Not an valid RESP message
		return data, fmt.Errorf("Not a valid RESP message")
	}

	switch string(dataByte) {
	case "$":
		{
			// This is a bulkString
			// fmt.Println("This is a bulk string byte")
			sizeByte, err := reader.ReadByte()
			// trim off the \r\n
			_, err = reader.ReadBytes('\n')
			if err != nil {
				// Not an valid RESP message
				return data, fmt.Errorf("No size byte in bulk string")
			}

			size, err := strconv.Atoi(string(sizeByte))

			if err != nil {
				// Not an valid RESP message
				return data, fmt.Errorf("Size byte is not number")
			}
			msg := make([]byte, size)
			reader.Read(msg)
			data.bulkString = string(msg)
			// fmt.Println("Message", string(msg))
			responses <- toRESPString("PONG")
			responseWg.Add(1)
			return data, nil

		}
	case "*":
		{
			// This is a Array
			sizeByte, err := reader.ReadByte()
			// trim off the \r\n
			_, err = reader.ReadBytes('\n')

			if err != nil {
				// Not an valid RESP message
				return data, fmt.Errorf("No size byte in array")
			}
			size, err := strconv.Atoi(string(sizeByte))

			if err != nil {
				// Not an valid RESP message
				return data, fmt.Errorf("Size byte is not number")
			}

			fmt.Println("Size byte", size)
			arrWg.Add(size)
			for i := 0; i < size; i++ {
				data, err := parseRedisData(reader)
				if err != nil {
					return data, err
				}
				data.array = append(data.array, data)
				arrWg.Done()
			}
			return data, nil
		}
	case "\r":
	case "\n":
		// If the byte is part of a CLRF, return empty
		return data, nil
	default:
		return data, fmt.Errorf("Not a valid RESP message")
	}

	return data, nil
}

func handleConnection(conn net.Conn) {
	fmt.Println("Waiting for connections ...")

	reader := bufio.NewReader(conn)

	_, err := parseRedisData(*reader)

	if err != nil {
		fmt.Println("Error:", err)
		conn.Write([]byte("Invalid RESP\n"))
		return
	}
	go writeResponse(conn)
	responseWg.Wait()
	arrWg.Wait()
	defer conn.Close()
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Calling handleConnection")
		go handleConnection(conn)
	}
}
