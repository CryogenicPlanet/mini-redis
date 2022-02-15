package main

import (
	"bufio"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

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

func nilRESP() string {
	return "$-1\r\n"
}

func writeResponse(writer *bufio.Writer, response string) {
	fmt.Print("Response", response)
	writer.Write([]byte(response))
	writer.Flush()
	responseWg.Done()
}

type commandType int

var database map[string]entry

type entry struct {
	hasEntry   bool
	value      string
	expiryTime time.Time
}

const (
	None commandType = iota
	PING
	ECHO
	SET
	GET
)

type redisCmd struct {
	commandType commandType
	redisData   []string
}

func parseCommandType(msg string) commandType {
	switch strings.ToUpper(msg) {
	case "PING":
		return PING
	case "ECHO":
		return ECHO
	case "SET":
		return SET
	case "GET":
		return GET
	default:
		return None
	}
}

// Expect every message to be in RESP and be an Array of bulk strings
func parseRedisData(scanner *bufio.Scanner) (redisCmd, error) {
	data := redisCmd{commandType: None}

	dataLine := scanner.Text()

	dataByte := dataLine[0:1]
	sizeByte := dataLine[1:2]
	fmt.Println("Data byte", string(dataByte))

	switch string(dataByte) {
	case "$":
		{
			if scanner.Scan() {
				msg := scanner.Text()
				data.redisData = append(data.redisData, msg)
				fmt.Println("Message", string(msg))
				if data.commandType == None {
					// Command Type unset and should be set
					cmdType := parseCommandType(msg)
					data.commandType = cmdType
				}
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

			arrWg.Add(size)
			if size == 1 {
				if scanner.Scan() {

					newData, err := parseRedisData(scanner)
					if err != nil {
						return data, err
					}
					if data.commandType == None {
						data.commandType = newData.commandType
					}
					data.redisData = append(data.redisData, newData.redisData...)
					arrWg.Done()
				}
			} else {
				for i := 0; i < size; i++ {
					if scanner.Scan() {

						newData, err := parseRedisData(scanner)
						if err != nil {
							fmt.Println("Error in recursion", err)
							return data, err
						}
						if data.commandType == None {
							data.commandType = newData.commandType
						}
						data.redisData = append(data.redisData, newData.redisData...)
						arrWg.Done()
						fmt.Println("Finishing loop", i)
					}

				}
				fmt.Println("Looped data", data)
			}
			return data, nil

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

func executeRedisData(redisCmd redisCmd, writer *bufio.Writer) error {
	switch redisCmd.commandType {
	case PING:
		responseVal := "PONG"
		if len(redisCmd.redisData) > 1 {
			responseVal = string(redisCmd.redisData[1]) // second param
		}
		go writeResponse(writer, toRESPString(responseVal))
		responseWg.Add(1)
		return nil
	case ECHO:
		responseVal := string(redisCmd.redisData[1]) // second param
		go writeResponse(writer, toRESPString(responseVal))
		responseWg.Add(1)
		return nil
	case SET:
		key := string(redisCmd.redisData[1]) // second param
		val := redisCmd.redisData[2]         // third param
		var px string
		expiry := 0
		if len(redisCmd.redisData) > 3 {
			px = string(redisCmd.redisData[3])
			expiry, _ = strconv.Atoi(string(redisCmd.redisData[4]))
		}
		hasExpiry := strings.ToUpper(px) == "PX"

		if _, ok := database[key]; !ok {
			database[key] = entry{hasEntry: hasExpiry, value: val, expiryTime: time.Now().Add(time.Millisecond * time.Duration(expiry))}
		}

		database[key] = entry{hasEntry: hasExpiry, value: val, expiryTime: time.Now().Add(time.Millisecond * time.Duration(expiry))}
		go writeResponse(writer, toRESPString("OK"))
		responseWg.Add(1)
		return nil
	case GET:
		key := string(redisCmd.redisData[1]) // second param
		if _, ok := database[key]; !ok {
			go writeResponse(writer, nilRESP())
			responseWg.Add(1)
			return nil
		}
		entry := database[key]
		if entry.hasEntry {
			if entry.expiryTime.Before(time.Now()) {
				delete(database, key)
				go writeResponse(writer, nilRESP())
				responseWg.Add(1)
				return nil
			}
		}

		go writeResponse(writer, toRESPString(string(entry.value)))
		responseWg.Add(1)
		return nil

	default:
		return fmt.Errorf("Invalid command, nothing to execute", redisCmd.commandType)
	}
}

func handleConnection(conn net.Conn) {
	fmt.Println("Waiting for connections ...")

	writer := bufio.NewWriter(conn)
	reader := bufio.NewReader(conn)
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "*") {
			data, err := parseRedisData(scanner)
			executeRedisData(data, writer)
			fmt.Println("Data is", data)
			if err != nil {
				fmt.Println("Error:", err)
				conn.Write([]byte("Invalid RESP\n"))
				return
			}
		}
	}

	arrWg.Wait()
	responseWg.Wait()

}

func main() {
	database = make(map[string]entry)
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
