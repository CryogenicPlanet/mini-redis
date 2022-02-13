package main

import (
	"bufio"
	"fmt"
	"log"
	"strings"

	// Uncomment this block to pass the first stage
	"net"
	"os"
)

func toRESPString(message string) string {
	return "+" + message + "\r\n"
}

// Expect every message to be in RESP and be an Array of bulk strings
func handleMessage(message string) (string, error) {

	splits := strings.Split(message, "\r\n")

	fmt.Println("Splits", splits)

	firstSplit := []rune(splits[0])

	if firstSplit[0] != '*' {
		// Not an valid RESP message
		return "", fmt.Errorf("Not a valid RESP message")
	}

	arrSize := int(firstSplit[1])

	if arrSize == 0 {
		return "", fmt.Errorf("Null RESP Array")
	}

	return toRESPString("PONG"), nil
}

func handleConnection(conn net.Conn) {
	fmt.Println("Waiting for connections ...")
	message, err := bufio.NewReader(conn).ReadString('\n')
	defer conn.Close()

	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Println("Message", message)
	response, err := handleMessage(message)

	if err != nil {
		fmt.Println("Error:", err)
		conn.Write([]byte("Invalid RESP\n"))
		return
	}
	fmt.Println("Response", response)

	conn.Write([]byte(response))
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
