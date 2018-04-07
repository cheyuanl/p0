package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
)

const (
	defaultHost = "localhost"
	defaultPort = 9999
)

// To test your server implementation, you might find it helpful to implement a
// simple 'client runner' program. The program could be very simple, as long as
// it is able to connect with and send messages to your server and is able to
// read and print out the server's response to standard output. Whether or
// not you add any code to this file will not affect your grade.
func main() {

	if addr, err := net.ResolveTCPAddr("tcp", defaultHost+":"+strconv.Itoa(defaultPort)); err != nil {
		panic("Cannot resolve ")
	} else if conn, err := net.DialTCP("tcp", nil, addr); err != nil {
		panic("Cannot dial ")
	} else {
		userInput := bufio.NewReader(os.Stdin)
		response := bufio.NewReader(conn)
		go func() {
			for {
				serverLine, err := response.ReadBytes(byte('\n'))
				switch err {
				case nil:
					fmt.Print(string(serverLine))
				case io.EOF:
					os.Exit(0)
				default:
					fmt.Println("ERROR", err)
					os.Exit(2)
				}
			}

		}()
		for {
			userLine, err := userInput.ReadBytes(byte('\n'))
			switch err {
			case nil:
				conn.Write(userLine)
			case io.EOF:
				os.Exit(0)
			default:
				fmt.Println("ERROR", err)
				os.Exit(1)
			}
		}

	}

}
