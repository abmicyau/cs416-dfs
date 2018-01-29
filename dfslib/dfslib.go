/*

This package specifies the application's interface to the distributed
file system (DFS) system to be used in assignment 2 of UBC CS 416
2017W2.

*/

package dfslib

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/rpc"
	"os"
	"time"
)

const CHUNK_SIZE = 32 // Chunk size in bytes
const FILE_SIZE = 256 // File size in Chunks
const CLIENT_TIMEOUT = 2000000000 // Client timeout in nanoseconds

var EMPTY_CHUNK Chunk

// A Chunk is the unit of reading/writing in DFS.
type Chunk [CHUNK_SIZE]byte

// Represents a type of file access.
type FileMode int

const (
	// Read mode.
	READ FileMode = iota

	// Read/Write mode.
	WRITE

	// Disconnected read mode.
	DREAD
)

////////////////////////////////////////////////////////////////////////////////////////////
// <ERROR DEFINITIONS>

// These type definitions allow the application to explicitly check
// for the kind of error that occurred. Each API call below lists the
// errors that it is allowed to raise.
//
// Also see:
// https://blog.golang.org/error-handling-and-go
// https://blog.golang.org/errors-are-values

// Contains serverAddr
type DisconnectedError string

func (e DisconnectedError) Error() string {
	return fmt.Sprintf("DFS: Not connected to server [%s]", string(e))
}

// Contains chunkNum that is unavailable
type ChunkUnavailableError uint8

func (e ChunkUnavailableError) Error() string {
	return fmt.Sprintf("DFS: Latest verson of chunk [%d] unavailable", e)
}

// Contains filename
type OpenWriteConflictError string

func (e OpenWriteConflictError) Error() string {
	return fmt.Sprintf("DFS: Filename [%s] is opened for writing by another client", string(e))
}

// Contains file mode that is bad.
type BadFileModeError FileMode

func (e BadFileModeError) Error() string {
	return fmt.Sprintf("DFS: Cannot perform this operation in current file mode [%d]", e)
}

// Contains filename.
type WriteModeTimeoutError string

func (e WriteModeTimeoutError) Error() string {
	return fmt.Sprintf("DFS: Write access to filename [%s] has timed out; reopen the file", string(e))
}

// Contains filename
type BadFilenameError string

func (e BadFilenameError) Error() string {
	return fmt.Sprintf("DFS: Filename [%s] includes illegal characters or has the wrong length", string(e))
}

// Contains filename
type FileUnavailableError string

func (e FileUnavailableError) Error() string {
	return fmt.Sprintf("DFS: Filename [%s] is unavailable", string(e))
}

// Contains local path
type LocalPathError string

func (e LocalPathError) Error() string {
	return fmt.Sprintf("DFS: Cannot access local path [%s]", string(e))
}

// Contains filename
type FileDoesNotExistError string

func (e FileDoesNotExistError) Error() string {
	return fmt.Sprintf("DFS: Cannot open file [%s] in D mode as it does not exist locally", string(e))
}

// </ERROR DEFINITIONS>
////////////////////////////////////////////////////////////////////////////////////////////

// Represents a file in the DFS system.
type DFSFile interface {
	// Reads chunk number chunkNum into storage pointed to by
	// chunk. Returns a non-nil error if the read was unsuccessful.
	//
	// Can return the following errors:
	// - DisconnectedError (in READ,WRITE modes)
	// - ChunkUnavailableError (in READ,WRITE modes)
	Read(chunkNum uint8, chunk *Chunk) (err error)

	// Writes chunk number chunkNum from storage pointed to by
	// chunk. Returns a non-nil error if the write was unsuccessful.
	//
	// Can return the following errors:
	// - BadFileModeError (in READ,DREAD modes)
	// - DisconnectedError (in WRITE mode)
	// - WriteModeTimeoutError (in WRITE mode)
	Write(chunkNum uint8, chunk *Chunk) (err error)

	// Closes the file/cleans up. Can return the following errors:
	// - DisconnectedError
	Close() (err error)
}

// Represents a connection to the DFS system.
type DFS interface {
	// Check if a file with filename fname exists locally (i.e.,
	// available for DREAD reads).
	//
	// Can return the following errors:
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	LocalFileExists(fname string) (exists bool, err error)

	// Check if a file with filename fname exists globally.
	//
	// Can return the following errors:
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	// - DisconnectedError
	GlobalFileExists(fname string) (exists bool, err error)

	// Opens a filename with name fname using mode. Creates the file
	// in READ/WRITE modes if it does not exist. Returns a handle to
	// the file through which other operations on this file can be
	// made.
	//
	// Can return the following errors:
	// - OpenWriteConflictError (in WRITE mode)
	// - DisconnectedError (in READ,WRITE modes)
	// - FileUnavailableError (in READ,WRITE modes)
	// - FileDoesNotExistError (in DREAD mode)
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	Open(fname string, mode FileMode) (f DFSFile, err error)

	// Disconnects from the server. Can return the following errors:
	// - DisconnectedError
	UMountDFS() (err error)
}

// Argument and reponse structs for RPC methods on the server
//
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

// The constructor for a new DFS object instance. Takes the server's
// IP:port address string as parameter, the localIP to use to
// establish the connection to the server, and a localPath path on the
// local filesystem where the client has allocated storage (and
// possibly existing state) for this DFS.
//
// The returned dfs instance is singleton: an application is expected
// to interact with just one dfs at a time.
//
// This call should succeed regardless of whether the server is
// reachable. Otherwise, applications cannot access (local) files
// while disconnected.
//
// Can return the following errors:
// - LocalPathError
// - Networking errors related to localIP or serverAddr
func MountDFS(serverAddr string, localIP string, localPath string) (fs DFS, err error) {
	client := new(Client)
	client.disconnected = true
	client.serverAddr = serverAddr
	client.localIP = localIP
	client.localPath = localPath
	client.mounted = true
	rpc.Register(client)

	err = validateLocalPath(localPath)
	if err != nil {
		return nil, err
	}

	client.greetServer()
	go client.heartbeat()

	return dfs{client}, nil
}

//

// The Client struct implements remotely callable methods for RPC and holds
// most of the state of the client in the distributed file system.
//
// TODO: make changes to the disconnected property thread safe X_X ...
//
type Client struct {
	clientID     string
	disconnected bool
	server       *rpc.Client
	serverAddr   string
	localIP      string
	localPath    string
	mounted      bool
}

// RPC method called from the DFS server to request a file chunk
//
func (c *Client) GetChunk(args *RPCChunkData, reply *Chunk) error {
	path := c.localPath + args.FileName + ".dfs"
	var chunk Chunk
	readChunk(path, &chunk, args.ChunkNum)
	*reply = chunk

	return nil
}

func (c *Client) heartbeat() {
	timeChan := time.Tick(500 * time.Millisecond)
	for _ = range timeChan {
		if !c.mounted {
			return
		}
		if c.disconnected {
			c.greetServer()
		} else {
			timeout := make(chan struct{})
			done := make(chan *rpc.Call, 1)
			go func() {
				time.Sleep(time.Nanosecond*CLIENT_TIMEOUT)
				close(timeout)
			}()
			c.server.Go("Server.Heartbeat", c.clientID, nil, done)
			go func() {
				select {
				case <-done:
					break
				case <-timeout:
					c.disconnected = true
				}
			}()
		}
	}
}

func (c *Client) greetServer() {
	if !c.disconnected {
		return
	}

	localAddr, err := acceptServerRPC(c.localIP)
	if err != nil {
		return
	}

	server, err := rpc.Dial("tcp", c.serverAddr)
	if err == nil {
		clientIDPath := c.localPath + ".clientid"
		clientID := getClientID(clientIDPath)

		args := &RPCHelloData{clientID, localAddr}
		var reply string
		err = server.Call("Server.Hello", args, &reply)
		checkError(err)

		if len(reply) > 0 {
			c.disconnected = false
			c.server = server
			c.clientID = reply
			if len(clientID) == 0 {
				storeClientID(reply, clientIDPath)
			}
		} else {
			c.disconnected = true
			c.mounted = false
		}
	}
}

//

type dfs struct {
	client *Client
}

// Check if a file with filename fname exists locally (i.e.,
// available for DREAD reads).
//
// Can return the following errors:
// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
func (d dfs) LocalFileExists(fname string) (exists bool, err error) {
	if !isValidFilename(fname) {
		return false, BadFilenameError(fname)
	}
	return checkFileOrDirectory(d.client.localPath + fname + ".dfs")
}

// Check if a file with filename fname exists globally.
//
// Can return the following errors:
// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
// - DisconnectedError
func (d dfs) GlobalFileExists(fname string) (exists bool, err error) {
	client := d.client
	server := client.server

	if !isValidFilename(fname) {
		return false, BadFilenameError(fname)
	} else if client.disconnected {
		return false, DisconnectedError(client.serverAddr)
	}

	err = server.Call("Server.FileExists", fname, &exists)
	checkError(err)

	return exists, nil
}

// Opens a filename with name fname using mode. Creates the file
// in READ/WRITE modes if it does not exist. Returns a handle to
// the file through which other operations on this file can be
// made.
//
// Can return the following errors:
// - OpenWriteConflictError (in WRITE mode)
// - DisconnectedError (in READ,WRITE modes)
// - FileUnavailableError (in READ,WRITE modes)
// - FileDoesNotExistError (in DREAD mode)
// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
func (d dfs) Open(fname string, mode FileMode) (f DFSFile, err error) {
	client := d.client
	server := client.server
	args := &RPCChunkData{client.clientID, fname, 0, false, false, EMPTY_CHUNK}

	if !isValidFilename(fname) {
		return nil, BadFilenameError(fname)
	} else if client.disconnected && mode != DREAD {
		return nil, DisconnectedError(client.serverAddr)
	}

	file := dfsFile{fname, mode, &d, new(bool)}
	filePath := client.localPath + fname + ".dfs"

	// At this point, we are either connected, or we are disconnected in DREAD
	if !client.disconnected {
		// Create the file if it doesn't exist
		err = server.Call("Server.NewFile", fname, nil)
		checkError(err)

		// Obtain write permissions
		if mode == WRITE {
			var canWrite bool
			err = server.Call("Server.RequestWrite", args, &canWrite)
			checkError(err)
			if !canWrite {
				return nil, OpenWriteConflictError(fname)
			}
		}

		// Try to fetch the latest contents of the file
		fptr := openFile(filePath)
		defer fptr.Close()

		reply := new(RPCFileData)
		err = server.Call("Server.GetFile", args, reply)
		checkError(err)

		if !reply.NewFile && !reply.Trivial {
			for i := 0; i < FILE_SIZE; i++ {
				if reply.NewChunks[i] {
					writeChunk(fptr, &reply.Chunks[i], uint8(i))
				}
			}
		} else if !reply.NewFile && mode != DREAD {
			return nil, FileUnavailableError(fname)
		}
	} else {
		// disconnected and in DREAD mode
		exists, err := checkFileOrDirectory(filePath)
		checkError(err)
		if !exists {
			return nil, FileDoesNotExistError(fname)
		}
	}

	return file, nil
}

func (d dfs) UMountDFS() (err error) {
	client := d.client
	server := client.server

	if client.disconnected {
		return DisconnectedError(client.serverAddr)
	} else {
		err = server.Call("Server.Unmount", client.clientID, nil)
		checkError(err)
	}

	return nil
}

//

type dfsFile struct {
	fname   string
	mode    FileMode
	fs      *dfs
	invalid *bool
}

func (f dfsFile) Read(chunkNum uint8, chunk *Chunk) (err error) {
	client := f.fs.client
	path := client.localPath + f.fname + ".dfs"

	if client.disconnected || *f.invalid {
		if f.mode != DREAD {
			*f.invalid = true
			return DisconnectedError(client.serverAddr)
		}
		readChunk(path, chunk, chunkNum)
	} else {
		server := client.server
		args := &RPCChunkData{client.clientID, f.fname, chunkNum, false, false, EMPTY_CHUNK}

		reply := new(RPCChunkData)
		err = server.Call("Server.GetChunk", args, reply)
		checkError(err)

		if !reply.NewChunk {
			// latest chunk is owned by the client
			readChunk(path, chunk, chunkNum)
		} else if reply.Success {
			*chunk = reply.Data
			file := openFile(path)
			defer file.Close()
			writeChunk(file, chunk, chunkNum)
		} else if f.mode == DREAD {
			readChunk(path, chunk, chunkNum)
		} else {
			return ChunkUnavailableError(chunkNum)
		}
	}

	return nil
}

func (f dfsFile) Write(chunkNum uint8, chunk *Chunk) (err error) {
	client := f.fs.client
	server := client.server
	if f.mode != WRITE {
		return BadFileModeError(f.mode)
	} else if client.disconnected || *f.invalid {
		*f.invalid = true
		return DisconnectedError(client.serverAddr)
	}

	// TODO: add write logs
	args := &RPCChunkData{client.clientID, f.fname, chunkNum, false, false, EMPTY_CHUNK}
	var ok bool
	err = server.Call("Server.WriteChunk", args, &ok)
	checkError(err)

	if ok {
		file := openFile(client.localPath + f.fname + ".dfs")
		defer file.Close()
		writeChunk(file, chunk, chunkNum)
	} else {
		return WriteModeTimeoutError(f.fname)
	}

	return nil
}

func (f dfsFile) Close() (err error) {
	client := f.fs.client
	if client.disconnected {
		return DisconnectedError(client.serverAddr)
	}

	server := client.server
	args := &RPCChunkData{client.clientID, f.fname, 0, false, false, EMPTY_CHUNK}
	err = server.Call("Server.CloseFile", args, nil)
	checkError(err)

	*f.invalid = true

	return nil
}

//

///////////////////////////
// HELPER/UTIL FUNCTIONS //
///////////////////////////

func getClientID(clientIDPath string) (clientID string) {
	clientIDExists, err := checkFileOrDirectory(clientIDPath)
	checkError(err)

	if clientIDExists {
		id, err := ioutil.ReadFile(clientIDPath)
		checkError(err)
		clientID = string(id)
	}

	return clientID
}

func storeClientID(clientID, clientIDPath string) {
	f, err := os.Create(clientIDPath)
	checkError(err)
	defer f.Close()

	data := []byte(clientID)
	_, err = f.Write(data)
	checkError(err)

	f.Sync()
}

func acceptServerRPC(localIP string) (localAddr string, err error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", localIP+":0")
	checkError(err)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return "", err
	}

	go func() {
		conn, err := listener.Accept()
		checkError(err)
		rpc.ServeConn(conn)
	}()

	return listener.Addr().String(), err
}

func checkFileOrDirectory(path string) (exists bool, err error) {
	_, err = os.Stat(path)
	if err == nil {
		exists = true
	} else if os.IsNotExist(err) {
		exists = false
		err = nil
	}

	return exists, err
}

func validateLocalPath(localPath string) error {
	localPathExists, err := checkFileOrDirectory(localPath)
	if !localPathExists || err != nil {
		return LocalPathError(localPath)
	} else {
		return nil
	}
}

func isValidFilename(fname string) bool {
	if len(fname) == 0 || len(fname) > 16 {
		return false
	}

	for i := 0; i < len(fname); i++ {
		if !(fname[i] >= '0' && fname[i] <= '9') && !(fname[i] >= 'a' && fname[i] <= 'z') {
			return false
		}
	}

	return true
}

// Opens a file from the local path, creating it if it doesn't exist.
// Remember to defer Close() after opening the file.
// Opens the file with write permissions.
//
func openFile(path string) (file *os.File) {
	exists, err := checkFileOrDirectory(path)
	checkError(err)

	if exists {
		file, err = os.OpenFile(path, os.O_WRONLY, 0666)
		checkError(err)
	} else {
		file, err = os.Create(path)
		checkError(err)
		file.Write(make([]byte, CHUNK_SIZE*FILE_SIZE))
		file.Sync()
	}

	return file
}

func writeChunk(file *os.File, chunk *Chunk, chunkNum uint8) {
	_, err := file.WriteAt(chunk[:], int64(chunkNum*CHUNK_SIZE))
	checkError(err)
	file.Sync()
}

func readChunk(path string, chunk *Chunk, chunkNum uint8) {
	file, err := os.Open(path)
	checkError(err)
	defer file.Close()
	_, err = file.ReadAt(chunk[:], int64(chunkNum*CHUNK_SIZE))
	checkError(err)
}

// If error is non-nil, print it out and return it.
func checkError(err error) error {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return err
	}
	return nil
}
