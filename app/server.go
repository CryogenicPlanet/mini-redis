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

var Yellow = "\033[33m"
var Reset = "\033[0m"

var responseWg sync.WaitGroup
var arrWg sync.WaitGroup

func toRESPString(message string) string {
	return "+" + message + "\r\n"
}

func writeResponse(writer *bufio.Writer, response string) {
	fmt.Print("Response", response)
	writer.Write([]byte(response))
	writer.Flush()
	responseWg.Done()
}

type redisData struct {
	simpleString string
	errorString  []byte
	bulkString   string
	integer      int
	array        []redisData
}

// Expect every message to be in RESP and be an Array of bulk strings
func parseRedisData(scanner *bufio.Scanner, writer *bufio.Writer) (redisData, error) {
	data := redisData{}

	dataLine := scanner.Text()

	dataByte := dataLine[0:1]
	sizeByte := dataLine[1:2]
	fmt.Println("Data byte", string(dataByte))

	switch string(dataByte) {
	case "$":
		{
			if scanner.Scan() {
				msg := scanner.Text()
				data.bulkString = string(msg)
				fmt.Println("Message", string(msg))
				go writeResponse(writer, toRESPString("PONG"))
				responseWg.Add(1)
				return data, nil
			} else {
				return data, fmt.Errorf("Could not read message in bulk string")
			}
		}
	case "*":
		{
			size, err := strconv.Atoi(string(sizeByte))

			if err != nil {
				// Not an valid RESP message
				return data, fmt.Errorf("Size byte is not number")
			}

			fmt.Println("Size byte", size)

			if scanner.Scan() {

				arrWg.Add(size)
				if size == 1 {

					data, err := parseRedisData(scanner, writer)
					if err != nil {
						return data, err
					}
					data.array = append(data.array, data)
					arrWg.Done()
				} else {
					for i := 0; i < size; i++ {
						data, err := parseRedisData(scanner, writer)
						if err != nil {
							return data, err
						}
						data.array = append(data.array, data)
						arrWg.Done()
					}
				}
				return data, nil
			} else {
				return data, fmt.Errorf("Could not read data in array")
			}
		}
	case "\r":
	case "\n":
		// If the byte is part of a CLRF, return empty
		return data, nil
	default:

		return data, fmt.Errorf("Not a valid RESP message, did not have a valid data byte")
	}

	return data, fmt.Errorf("Unreachable code")
}

func handleConnection(conn net.Conn) {
	fmt.Println("Waiting for connections ...")

	writer := bufio.NewWriter(conn)
	reader := bufio.NewReader(conn)
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		_, err := parseRedisData(scanner, writer)
		if err != nil {
			fmt.Println("Error:", err)
			conn.Write([]byte("Invalid RESP\n"))
			return
		}
	}
	arrWg.Wait()
	responseWg.Wait()

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
