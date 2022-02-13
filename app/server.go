package main

import (
	"bufio"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	// Uncomment this block to pass the first stage
	"net"
	"os"
)

var responses = make(chan string, 10)
var responseWg sync.WaitGroup

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

// Expect every message to be in RESP and be an Array of bulk strings
func handleMessage(message string) error {

	splits := strings.Split(message, `\r\n`)

	fmt.Println("Splits", splits)

	firstSplit := []rune(splits[0])

	if firstSplit[0] != '*' {
		// Not an valid RESP message
		return fmt.Errorf("Not a valid RESP message")
	}

	arrSize, err := strconv.Atoi(string(firstSplit[1]))

	fmt.Println("Arr size", arrSize, firstSplit)

	if arrSize == 0 || err != nil {
		return fmt.Errorf("Null RESP Array")
	}

	if arrSize > 1 {
		// Have to loop over messages
		for i := 0; i < arrSize; i++ {
			fmt.Println("Responded to X PING", i)
			responses <- toRESPString("PONG")
			responseWg.Add(1)
		}
		return nil
	}

	responses <- toRESPString("PONG")
	responseWg.Add(1)

	return nil
}

func handleConnection(conn net.Conn) {
	fmt.Println("Waiting for connections ...")
	message, err := bufio.NewReader(conn).ReadString('\n')

	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Println("Message", message)
	err = handleMessage(message)

	if err != nil {
		fmt.Println("Error:", err)
		conn.Write([]byte("Invalid RESP\n"))
		return
	}
	go writeResponse(conn)
	responseWg.Wait()
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
