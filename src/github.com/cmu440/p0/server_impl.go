package p0

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

const (
	inboxBuffSize = 500
	putOp         = "put"
	getOp         = "get"
	addCli        = "addCli"
	delCli        = "delCli"
)

type client struct {
	conn  net.Conn
	inBox chan []byte // the msg to be sent to a client. if it is full, simply drop it
	done  bool
}
type kvPair struct {
	key   string
	value []byte
}

type cliReq struct {
	op  string
	kvp *kvPair
}

type cliIO struct {
	op  string
	cli *client
}

type keyValueServer struct {
	listener net.Listener
	clients  map[*client]bool // store the connection info

	cliReqChan    chan cliReq // channel for client request
	cliIOChan     chan cliIO
	broadcastChan chan []byte // channel for message to be broadcasted

	done bool
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	return &keyValueServer{
		clients:       make(map[*client]bool),
		cliReqChan:    make(chan cliReq),
		broadcastChan: make(chan []byte),
		cliIOChan:     make(chan cliIO),
	}
}

// It is responsible for publishing the messages in the broadcastChan
func (kvs *keyValueServer) Broadcast() {
	for !kvs.done {
		msg := <-kvs.broadcastChan
		for cli := range kvs.clients {
			// drop the msg if the inbox is full
			if len(cli.inBox) < inboxBuffSize {
				cli.inBox <- msg
			}
		}
	}
}

// It is responsible for accepting the connection and append,
// and delete the connection list. There should be only one thread running this function.
func (kvs *keyValueServer) AcceptClients() {
	defer kvs.listener.Close()

	for !kvs.done {
		conn, err := kvs.listener.Accept()
		if conn == nil {
			fmt.Println(err)
			continue
		}

		cli := &client{conn: conn, inBox: make(chan []byte, inboxBuffSize)}
		kvs.cliIOChan <- cliIO{addCli, cli}
		go kvs.HandleClient(cli)
	}

}

func (kvs *keyValueServer) UpdateKvStore() {
	for !kvs.done {
		switch cliReq := <-kvs.cliReqChan; cliReq.op {

		case putOp:
			put(cliReq.kvp.key, cliReq.kvp.value)
		case getOp:
			value := get(cliReq.kvp.key)
			kvs.broadcastChan <- bytes.Join([][]byte{[]byte(cliReq.kvp.key), value}, []byte(","))
		default:
			panic("something when wrong in cliReq")
		}
	}
}

func (kvs *keyValueServer) ManageClients() {
	for !kvs.done {
		cliIO := <-kvs.cliIOChan
		switch cliIO.op {
		case addCli:
			kvs.clients[cliIO.cli] = true
		case delCli:
			cliIO.cli.conn.Close()
			delete(kvs.clients, cliIO.cli)
		}
	}
}

func (kvs *keyValueServer) Start(port int) error {
	// create a listener
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if ln == nil {
		return err
	}

	// initialization
	kvs.listener = ln
	init_db()

	go kvs.AcceptClients()

	go kvs.ManageClients()

	go kvs.Broadcast()

	go kvs.UpdateKvStore()

	return err
}

func (kvs *keyValueServer) Close() {
	kvs.done = true
}

func (kvs *keyValueServer) Count() int {
	return len(kvs.clients)
}

func inBoxConsumer(cli *client) {
	for !cli.done {
		msg := <-cli.inBox
		if _, err := cli.conn.Write(append([]byte(msg), []byte("\n")...)); err != nil {
			panic(err)
		}
	}
}

func (kvs *keyValueServer) HandleClient(cli *client) {
	defer func() {
		// remove itself from *client list
		cli.done = true
		kvs.cliIOChan <- cliIO{delCli, cli}
	}()

	go inBoxConsumer(cli)
	reader := bufio.NewReader(cli.conn)

	for !kvs.done {
		msg, err := reader.ReadBytes('\n')
		if err == io.EOF {
			return
		} else if err != nil {
			fmt.Println("Something wrong, host should close connection", err, cli.conn)
			return
		}

		// TODO: better way to parse it?
		line := strings.Split(strings.TrimSuffix(string(msg), "\n"), ",")
		if len(line) < 2 {
			fmt.Println("Invalid request from", cli.conn)
			return
		}
		op := line[0]
		key := line[1]

		switch op {
		case putOp:
			if len(line) != 3 {
				fmt.Println("Invalid request", cli.conn)
				return
			}
			value := []byte(line[2])
			kvs.cliReqChan <- cliReq{putOp, &kvPair{key, value}}
		case getOp:
			kvs.cliReqChan <- cliReq{getOp, &kvPair{key, nil}}
		default:
			fmt.Println("Invalid request", cli.conn)
		}
	}
}
