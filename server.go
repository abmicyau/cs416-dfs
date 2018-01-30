package main

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const CHUNK_SIZE = 32             // Chunk size in bytes
const FILE_SIZE = 256             // File size in Chunks
const MAX_CLIENTS = 16            // Max number of clients
const CID_LENGTH = 32             // Length of client IDs
const CLIENT_TIMEOUT = 2000000000 // Client timeout in nanoseconds
const LOG_ENABLED = false

var EMPTY_CHUNK Chunk
var ALPHABET = []rune("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
var lock sync.Mutex

// A Chunk is the unit of reading/writing in DFS.
type Chunk [CHUNK_SIZE]byte

func main() {
	server := new(Server)
	server.clients = make(map[string]*DFSClient)
	server.fileMap = make(map[string]*FileMetadata)
	rpc.Register(server)

	tcpAddr, err := net.ResolveTCPAddr("tcp", os.Args[1])
	checkError(err)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)

	rand.Seed(time.Now().Unix())

	for {
		conn, err := listener.Accept()
		checkError(err)
		logMessage("New connection from " + conn.RemoteAddr().String())
		go rpc.ServeConn(conn)
	}
}

////////////////////////////////////////////////////////////////////////////////////////////
// <RPC ARGUMENT/RESPONSE STRUCTS>

type RPCHelloData struct {
	ClientID, ClientIP string
}

type RPCFileData struct {
	ClientID  string
	FileName  string
	Trivial   bool
	NewFile   bool
	Chunks    [FILE_SIZE]Chunk
	NewChunks [FILE_SIZE]bool
}

type RPCChunkData struct {
	ClientID string
	FileName string
	ChunkNum uint8
	NewChunk bool
	Success  bool
	Data     Chunk
}

// </RPC ARGUMENT/RESPONSE STRUCTS>
////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////
// <OTHER TYPE DECLARATIONS>

type DFSClient struct {
	client        *rpc.Client
	clientID      string
	lastHeartbeat int64
	mounted       bool
}

type FileMetadata struct {
	fname         string
	writer        *DFSClient
	chunkOwners   []*DFSClient
	chunkVersions []int
	written       bool
}

type Server struct {
	numClients int
	clients    map[string]*DFSClient
	fileMap    map[string]*FileMetadata
}

// </OTHER TYPE DECLARATIONS>
////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////
// <EXPORTED METHODS>

// Initial greeting from a client.
//
// If a client ID is specified, then the server checks to make sure that it is valid.
// If the client ID is valid, then it is returned in newCID.
// If the client ID is invalid, then newCID is untouched.
//
// If a client ID is not specified, then the server begins the client registration process.
// If there is room for a new client, a new client ID is generated and returned in newCID.
// If there are already MAX_CLIENTS registered, then newCID is untouched.
//
// The client port is used to set up the bidirectional RPC.
//
//
func (s *Server) Hello(args *RPCHelloData, newCID *string) error {
	logMessage("Hello() called")
	lock.Lock()
	defer lock.Unlock()

	if len(args.ClientID) == 0 {
		if s.numClients < MAX_CLIENTS {
			*newCID = generateClientID(CID_LENGTH)
			s.mountClient(*newCID, args.ClientIP)
		}
	} else {
		client := s.clients[args.ClientID]
		if client != nil {
			s.mountClient(args.ClientID, args.ClientIP)
			*newCID = args.ClientID
		}
	}

	return nil
}

// Mounts a single client. The client is created if it doesn't exist.
//
func (s *Server) mountClient(id, ip string) {
	d := s.clients[id]
	if d == nil {
		d = new(DFSClient)
		d.clientID = id
		d.lastHeartbeat = time.Now().UnixNano()
		s.clients[id] = d
		s.numClients++
	}
	d.mounted = true
	client, err := rpc.Dial("tcp", ip)
	checkError(err)
	d.client = client
}

// Hearbeat method for connected clients, which will be called periodically and
// update the lastHeartbeat field to be used to detect failed clients.
//
func (s *Server) Heartbeat(id string, _ *struct{}) error {
	client := s.clients[id]
	if client == nil {
		return errors.New("Heartbeat: Client is not registered")
	}
	client.lastHeartbeat = time.Now().UnixNano()

	return nil
}

// Checks if a file exists anywhere in the file system
//
func (s *Server) FileExists(fname string, exists *bool) error {
	logMessage("FileExists() called")
	*exists = s.fileMap[fname] != nil
	return nil
}

// Creates a new file if it doesn't exist
//
func (s *Server) NewFile(fname string, _ *struct{}) error {
	logMessage("NewFile() called")
	lock.Lock()
	defer lock.Unlock()

	if s.fileMap[fname] == nil {
		s.fileMap[fname] = newFile(fname)
	}

	return nil
}

// Request to open a file in write mode
//
func (s *Server) RequestWrite(args *RPCChunkData, granted *bool) error {
	logMessage("RequestWrite() called")
	lock.Lock()
	defer lock.Unlock()

	fname := args.FileName
	client := s.clients[args.ClientID]
	if client == nil {
		return errors.New("RequestWrite: Client is not registered")
	}
	file := s.fileMap[fname]
	if file == nil {
		return errors.New("RequestWrite: File does not exist")
	}
	if file.writer == client || file.writer == nil || !isClientConnected(file.writer) {
		file.writer = client
		go pollWriter(client, file)
		*granted = true
	} else {
		*granted = false
	}

	return nil
}

// Attempt to fetch the most up-to-date version of each chunk of a file
//
func (s *Server) GetFile(args *RPCChunkData, reply *RPCFileData) error {
	logMessage("GetFile() called")
	lock.Lock()
	defer lock.Unlock()

	fname := args.FileName
	client := s.clients[args.ClientID]
	if client == nil {
		return errors.New("GetFile: Client is not registered")
	}
	file := s.fileMap[fname]
	if file == nil {
		return errors.New("GetFile: File does not exist")
	}

	if !file.written {
		reply.NewFile = true
	} else {
		reply.NewFile = false
		var chunks [FILE_SIZE]Chunk
		var newChunks [FILE_SIZE]bool
		trivial := true

		for i := 0; i < FILE_SIZE; i++ {
			owner := file.chunkOwners[i]
			if owner == client {
				trivial = false
			} else if owner != nil {
				chunk, err := requestChunk(owner, fname, uint8(i))
				if err == nil {
					trivial = false
					chunks[i] = chunk
					newChunks[i] = true
				}
			}
		}
		reply.Chunks = chunks
		reply.NewChunks = newChunks
		reply.Trivial = trivial
	}

	return nil
}

// Unmounts a dfs client
//
func (s *Server) Unmount(id string, _ *struct{}) error {
	logMessage("Unmount() called")
	client := s.clients[id]
	if client == nil {
		return errors.New("Unmount: Client is not registered")
	}
	client.mounted = false

	return nil
}

// Writes a chunk to a file in the DFS
//
func (s *Server) WriteChunk(args *RPCChunkData, ok *bool) error {
	logMessage("WriteChunk() called")
	lock.Lock()
	defer lock.Unlock()

	fname := args.FileName
	client := s.clients[args.ClientID]
	if client == nil {
		return errors.New("WriteChunk: Client is not registered")
	}
	file := s.fileMap[fname]
	if file == nil {
		return errors.New("WriteChunk: File does not exist")
	}

	if client == file.writer {
		file.written = true
		file.chunkOwners[args.ChunkNum] = client
		file.chunkVersions[args.ChunkNum]++
		*ok = true
		return nil
	} else {
		*ok = false
		return nil
	}
}

// Attempts to fetch the most up-to-date chunk of a file in the DFS
//
func (s *Server) GetChunk(args *RPCChunkData, reply *RPCChunkData) error {
	logMessage("GetChunk() called")

	fname := args.FileName
	client := s.clients[args.ClientID]
	if client == nil {
		return errors.New("ReadChunk: Client is not registered")
	}
	file := s.fileMap[fname]
	if file == nil {
		return errors.New("ReadChunk: File does not exist")
	}

	owner := file.chunkOwners[args.ChunkNum]

	// 'NewChunk' here indicates whether or not the requesting client already has
	// the most up-to-date version of the chunk. If the chunk is not new, then they
	// already have the most up-to-date version and the client is notified to fetch
	// the chunk from their local storage.
	if owner != nil {
		if owner == client {
			reply.NewChunk = false
			reply.Success = true
		} else {
			chunk, err := requestChunk(owner, fname, args.ChunkNum)
			if err == nil {
				reply.Data = chunk
				reply.NewChunk = true
				reply.Success = true
			} else {
				reply.NewChunk = true
				reply.Success = false
			}
		}
	} else {
		var chunk Chunk
		reply.Data = chunk
		reply.NewChunk = true
		reply.Success = true
	}

	return nil
}

// Closes a file. Releases write status if necessary.
//
func (s *Server) CloseFile(args *RPCChunkData, _ *struct{}) error {
	logMessage("CloseFile() called")
	lock.Lock()
	defer lock.Unlock()

	fname := args.FileName
	client := s.clients[args.ClientID]
	if client == nil {
		return errors.New("ReadChunk: Client is not registered")
	}
	file := s.fileMap[fname]
	if file == nil {
		return errors.New("ReadChunk: File does not exist")
	}
	if file.writer == client {
		file.writer = nil
	}

	return nil
}

// </EXPORTED METHODS>
////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////
// <OTHER METHODS>

// If error is non-nil, print it out and return it.
//
func checkError(err error) error {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return err
	}
	return nil
}

// Generates a random alphanumeric client ID of a specified length
//
func generateClientID(length int) string {
	id := make([]rune, length)
	for i := range id {
		id[i] = ALPHABET[rand.Intn(len(ALPHABET))]
	}
	return string(id)
}

// Checks whether a client is currently connected.
//
func isClientConnected(client *DFSClient) bool {
	if !client.mounted {
		return false
	} else {
		return time.Now().UnixNano()-client.lastHeartbeat < CLIENT_TIMEOUT
	}
}

// Initializes and returns a pointer to a struct containing empty file metadata
//
func newFile(fname string) *FileMetadata {
	file := new(FileMetadata)
	file.fname = fname
	file.writer = nil
	file.chunkOwners = make([]*DFSClient, FILE_SIZE)
	file.chunkVersions = make([]int, FILE_SIZE)
	file.written = false
	return file
}

// Requests a chunk from a client (using RPC)
//
func requestChunk(client *DFSClient, fname string, chunkNum uint8) (chunk Chunk, err error) {
	if isClientConnected(client) {
		args := &RPCChunkData{client.clientID, fname, chunkNum, false, false, EMPTY_CHUNK}
		err = client.client.Call("Client.GetChunk", args, &chunk)
	} else {
		err = errors.New("Client is not connected")
	}

	return chunk, err
}

// Continuously checks whether or not a client, which has requested to write a
// file, is currently online. If they have been disconnected for some time (longer
// than some specified timeout) then their write status is revoked.
//
func pollWriter(client *DFSClient, file *FileMetadata) {
	timeChan := time.Tick(500 * time.Millisecond)
	for _ = range timeChan {
		if !isClientConnected(client) {
			lock.Lock()
			defer lock.Unlock()

			file.writer = nil
			return
		}
	}
}

// Logging
//
func logMessage(msg string) {
	if LOG_ENABLED {
		fmt.Println(":" + strconv.Itoa(time.Now().Second()) + " ~ " + msg)
	}
}

// </OTHER METHODS>
////////////////////////////////////////////////////////////////////////////////////////////
