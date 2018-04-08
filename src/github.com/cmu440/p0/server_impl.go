/**
 * This key-value storage server handles multiple cliMngPool concurrently and broadcasts
 * the response to cliMngPool. The sychronizations are handled by golang built-in channels.
 *
 * The message flow works as follows:
 * 1. Fan-in stage
 * 	 Multiple clients produce requests to the cliReqChan.
 *   The cliReqChan is consumed by UpdateKvstore routine that runs in single thread to ensure
 *   serial access of hashtable.
 * 2. Fan-out stage
 *   UpdateKvstore produces the msg to the broadcastChan, which is consumed by Broatcast routine
 *   that send response to the minBoxChans of cliMngPool.
 *
 * @author Che-Yuan Liang Apr. 2018
 */

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

type kvPair struct {
	key   string
	value []byte
}

type client struct {
	conn  net.Conn
	inBox chan []byte // the message queue to be consumed by client
	done  bool
}

// The client request event
type cliReq struct {
	op  string
	kvp *kvPair
}

// The client management event
type cliMng struct {
	op  string
	cli *client
}

type keyValueServer struct {
	listener   net.Listener
	cliMngPool map[*client]bool // keep track of the connections

	cliReqChan    chan cliReq // channel for client request events
	cliMngChan    chan cliMng // cahnnel for client managment events
	broadcastChan chan []byte // channel for message to be broadcasted

	done bool
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	return &keyValueServer{
		cliMngPool:    make(map[*client]bool),
		cliReqChan:    make(chan cliReq),
		broadcastChan: make(chan []byte),
		cliMngChan:    make(chan cliMng),
	}
}

func (kvs *keyValueServer) Start(port int) error {
	// create a listener
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if ln == nil {
		return err
	}

	// init
	kvs.listener = ln
	init_db()

	// spawn routines
	go kvs.AcceptClients()

	// fan-in
	go kvs.ManageClients()

	go kvs.UpdateKvStore()

	// fan-out
	go kvs.Broadcast()

	return err
}

// shutdown all go routines and the server
func (kvs *keyValueServer) Close() {
	// destroy the routines created at start()
	kvs.done = true
}

func (kvs *keyValueServer) Count() int {
	return len(kvs.cliMngPool)
}

// Client consume the inbox
func (cli *client) ConsumeCliInbox() {
	for !cli.done {
		msg := <-cli.inBox
		if _, err := cli.conn.Write(append([]byte(msg), []byte("\n")...)); err != nil {
			panic(err)
		}
	}
}

func (kvs *keyValueServer) ProduceCliReq(cli *client) {
	// shut down client connction
	defer func() {
		// produce cliMng event
		kvs.cliMngChan <- cliMng{delCli, cli}
	}()

	reader := bufio.NewReader(cli.conn)

	for !kvs.done {
		// read bytes
		msg, err := reader.ReadBytes('\n')
		if err == io.EOF {
			return
		} else if err != nil {
			fmt.Println("Something wrong, host should close connection", err, cli.conn)
			return
		}

		// parse request
		line := strings.Split(strings.TrimSuffix(string(msg), "\n"), ",")
		if len(line) < 2 {
			fmt.Println("Invalid request sent from", cli.conn)
			return
		}
		op := line[0]
		key := line[1]

		// produce cliReq event
		switch op {
		case putOp:
			if len(line) != 3 {
				fmt.Println("Invalid request sent from", cli.conn)
				return
			}
			value := []byte(line[2])
			kvs.cliReqChan <- cliReq{putOp, &kvPair{key, value}}
		case getOp:
			kvs.cliReqChan <- cliReq{getOp, &kvPair{key, nil}}
		default:
			fmt.Println("Invalid request", cli.conn)
			return
		}
	}
}

// Consume the messages from broadcastChan and produce it to each inbox.
func (kvs *keyValueServer) Broadcast() {
	for !kvs.done {
		msg := <-kvs.broadcastChan
		for cli := range kvs.cliMngPool {
			if len(cli.inBox) < inboxBuffSize {
				cli.inBox <- msg
			}
			// drop the msg if the inbox is full
		}
	}
}

func (kvs *keyValueServer) AcceptClients() {
	if kvs.listener == nil {
		panic("listener is nil")
	}
	defer kvs.listener.Close()

	for !kvs.done {
		conn, err := kvs.listener.Accept()
		if conn == nil {
			fmt.Println(err)
			continue
		}

		cli := &client{conn: conn, inBox: make(chan []byte, inboxBuffSize)}
		kvs.cliMngChan <- cliMng{addCli, cli}

		go kvs.ProduceCliReq(cli)
		go cli.ConsumeCliInbox()
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
			panic("something when wrong with cliReq producer")
		}
	}
}

func (kvs *keyValueServer) ManageClients() {
	// destroy all client connections
	defer func() {
		for cli := range kvs.cliMngPool {
			cli.done = true
		}
	}()

	for !kvs.done {
		cliMng := <-kvs.cliMngChan
		switch cliMng.op {
		case addCli:
			kvs.cliMngPool[cliMng.cli] = true
		case delCli:
			cliMng.cli.done = true
			delete(kvs.cliMngPool, cliMng.cli)
		}
	}

}
