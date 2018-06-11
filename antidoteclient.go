package antidoteclient

import (
	"fmt"
	"math/rand"
	"net"
	"time"

	"gopkg.in/fatih/pool.v2"
)

const INITIAL_POOL_SIZE = 1
const MAX_POOL_SIZE = 50

// Represents connections to the Antidote database.
// Allows to start/create transaction.
type Client struct {
	pools []pool.Pool
}

// Represents an Antidote server.
// The port needs to be the port of the protocol-buffer interface (usually 8087)
type Host struct {
	Name string
	Port int
}

// Recreates a new Antidote client connected to the given Antidote servers.
// Remember to close the client to clean-up the connections in the connection pool
func NewClient(hosts ...Host) (client *Client, err error) {
	pools := make([]pool.Pool, len(hosts))
	for i, h := range hosts {
		p, err := pool.NewChannelPool(INITIAL_POOL_SIZE, MAX_POOL_SIZE, func() (net.Conn, error) { return net.Dial("tcp", fmt.Sprintf("%s:%d", h.Name, h.Port)) })
		if err != nil {
			return nil, err
		}
		pools[i] = p
	}
	client = &Client{
		pools: pools,
	}
	return
}

// Call close after using the client to clean up the connections int he connection pool and release resources.
func (client *Client) Close() {
	for _, p := range client.pools {
		p.Close()
	}
}

func (client *Client) getConnection() (c *connection, err error) {
	// maybe make this global?
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for _, i := range r.Perm(len(client.pools)) {
		p := client.pools[i]
		con, err := p.Get()
		if err != nil {
			return nil, err
		}
		c = &connection{
			Conn: con,
			pool: p,
		}
		return c, nil
	}
	err = fmt.Errorf("All connections dead")
	return
}

// a close already puts the connection back into the right pool
type connection struct {
	net.Conn
	pool pool.Pool
}

// Starts an interactive transaction and registers it on the Antidote server.
// The connection used to issue reads and updates is sticky;
// interactive transactions are only valid local to the server they are started on.
func (client *Client) StartTransaction() (tx *InteractiveTransaction, err error) {
	con, err := client.getConnection()
	if err != nil {
		return
	}
	readwrite := uint32(0)
	blue := uint32(0)
	apbtxn := &ApbStartTransaction{
		Properties: &ApbTxnProperties{ReadWrite: &readwrite, RedBlue: &blue},
	}
	err = apbtxn.encode(con)
	if err != nil {
		return
	}

	apbtxnresp, err := decodeStartTransactionResp(con)
	if err != nil {
		return
	}
	txndesc := apbtxnresp.TransactionDescriptor
	tx = &InteractiveTransaction{
		con:  con,
		txID: txndesc,
	}
	return
}

// Creates a static transaction object. Does not communicate with the Antidote server.
func (client *Client) CreateStaticTransaction() *StaticTransaction {
	return &StaticTransaction{client: client}
}
