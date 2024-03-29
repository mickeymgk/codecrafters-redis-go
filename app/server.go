package main

import (
	"fmt"
	// Uncomment this block to pass the first stage
	"flag"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Data struct {
	Value  string
	Expiry int64
}

type KV struct {
	Key    string
	Value  string
	Expiry int64
}

var db = make(map[string]Data)
var directory = ""
var fileName = ""

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	dir := flag.String("dir", "", "The directory where RDB files are stored")
	dbfilename := flag.String("dbfilename", "", "The name of the RDB file")

	flag.Parse()

	fmt.Println("dir:", *dir)
	fmt.Println("dbfilename:", *dbfilename)
	directory = *dir
	fileName = *dbfilename

	// Uncomment this block to pass the first stage
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	buf := make([]byte, 1024)
	for {
		_, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading from client: ", err.Error())
			return
		}
		str := string(buf)
		fmt.Println("comm :", str)
		response := ""
		chunks := strings.Split(str, "\r\n")
		command := chunks[2]
		switch command {
		case "echo":
			response = "+" + chunks[4] + "\r\n"
		case "set":
			key := chunks[4]
			value := chunks[6]
			unixMilli := time.Now().UnixMilli()
			offset, _ := strconv.ParseInt(chunks[len(chunks)-2], 10, 64)
			if offset > 0 {
				expiry := unixMilli + offset
				db[key] = Data{value, expiry}
			} else {
				db[key] = Data{value, 0}
			}
			response = "+OK\r\n"
		case "get":
			key := chunks[4]
			val := db[key]
			if fileName != "" {
				value := readKVs(directory + "/" + fileName)
				res := ""
				for _, kv := range value {
					if kv.Expiry > 0 && kv.Expiry <= time.Now().UnixMilli() {
						res += "$-1\r\n"
					} else {
						res += "$" + strconv.Itoa(len(kv.Value)) + "\r\n" + kv.Value + "\r\n"
					}
				}
				response = res
			} else {
				if val.Expiry > 0 && val.Expiry <= time.Now().UnixMilli() {
					delete(db, key)
					response = "$-1\r\n"
				} else {
					response = "+" + val.Value + "\r\n"
				}
			}
		case "config":
			if chunks[4] == "get" && chunks[6] == "dir" {
				response = "*2\r\n$3\r\ndir\r\n$" + strconv.Itoa(len(directory)) + "\r\n" + directory + "\r\n"
			}
		case "keys":
			if chunks[4] == "*" {
				content := readKVs(directory + "/" + fileName)
				res := "*" + strconv.Itoa(len(content))
				for _, kv := range content {
					res += "\r\n$" + strconv.Itoa(len(kv.Key)) + "\r\n" + kv.Key
				}
				res += "\r\n"
				response = res
			}
		default:
			response = "+PONG\r\n"
		}
		conn.Write([]byte(response))
	}
}

func indexOf(item byte, byteArray []byte) int {
	for i, value := range byteArray {
		if value == item {
			return i
		}
	}
	return -1
}

func parseTable(bytes []byte) []byte {
	start := indexOf(251, bytes)
	end := indexOf(255, bytes)
	return bytes[start+1 : end]
}

func readKVs(path string) []KV {
	content, _ := os.ReadFile(path)
	fmt.Println("MAGIC", string(content[:5]))
	fmt.Println("VERSION", string(content[5:9]))
	fmt.Println("TABLE", parseTable(content))
	fmt.Println("STRING", string(parseTable(content)))

	dbContent := parseTable(content)
	index := 1

	var expiries []int64

	for i := 0; i < len(dbContent); {
		if dbContent[i] == 252 {
			var expiry int64
			for j := 0; j < 8; j++ {
				expiry |= int64(dbContent[i+j+1]) << (8 * uint(j))
			}
			expiries = append(expiries, expiry)
			dbContent = append(dbContent[:i], dbContent[i+9:]...)
		} else {
			i++
		}
	}

	cleanedArray := make([]byte, 0)
	for _, b := range dbContent {
		if b != 252 {
			cleanedArray = append(cleanedArray, b)
		}
	}

	var kvChunks [][]byte
	for index < len(cleanedArray) {
		nextZeroIndex := index
		for nextZeroIndex < len(cleanedArray) && cleanedArray[nextZeroIndex] != 0 {
			nextZeroIndex++
		}
		if nextZeroIndex > index {
			if (len(cleanedArray[index:nextZeroIndex])) > 1 { //trim junk value
				kvChunks = append(kvChunks, cleanedArray[index:nextZeroIndex])
			}
		}
		index = nextZeroIndex + 1
	}

	var keys []KV
	for i, value := range kvChunks {
		keyLen := value[0]
		var expiry int64
		if len(expiries) > 0 {
			expiry = expiries[i]
		} else {
			expiry = 0
		}
		keys = append(keys, KV{string(value[1 : keyLen+1]), string(value[keyLen+2:]), expiry})
	}
	return keys
}
