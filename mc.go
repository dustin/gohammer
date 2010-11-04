package mc

import . "./mc_constants"
import . "./byte_manipulation"

import (
	"net"
	"log"
	"io"
	"bufio"
	"runtime"
	)

const bufsize = 1024

type MemcachedClient struct {
	Conn net.Conn
	writer *bufio.Writer
}

func Connect(prot string, dest string) (rv *MemcachedClient) {
	conn, err := net.Dial(prot, "", dest)
        if err != nil {
		log.Exitf("Failed to connect: %s", err)
	}
	rv = new(MemcachedClient)
	rv.Conn = conn
	rv.writer, err = bufio.NewWriterSize(rv.Conn, bufsize)
	if err != nil {
		panic("Can't make a buffer")
	}
	return rv
}

func send(client *MemcachedClient, req MCRequest) (rv MCResponse) {
	transmitRequest(client.writer, req)
	rv = getResponse(client)
	return
}

func Get(client *MemcachedClient, key string) MCResponse {
	var req MCRequest
	req.Opcode = GET
	req.Key = make([]byte, len(key))
	copy(req.Key, key)
	req.Cas = 0
	req.Opaque = 0
	req.Extras = make([]byte, 0)
	req.Body = make([]byte, 0)
	return send(client, req)
}

func Del(client *MemcachedClient, key string) MCResponse {
	var req MCRequest
	req.Opcode = DELETE
	req.Key = make([]byte, len(key))
	copy(req.Key, key)
	req.Cas = 0
	req.Opaque = 0
	req.Extras = make([]byte, 0)
	req.Body = make([]byte, 0)
	return send(client, req)
}

func store(client *MemcachedClient, opcode uint8,
	key string, flags int, exp int, body []byte) MCResponse {

	var req MCRequest
	req.Opcode = opcode
	req.Cas = 0
	req.Opaque = 0
	req.Key = make([]byte, len(key))
	copy(req.Key, key)
	req.Extras = WriteUint64(uint64(flags) << 32 | uint64(exp))
	req.Body = body
	return send(client, req)
}

func Add(client *MemcachedClient, key string, flags int, exp int,
	body []byte) MCResponse {
	return store(client, ADD, key, flags, exp, body)
}

func Set(client *MemcachedClient, key string, flags int, exp int,
	body []byte) MCResponse {
	return store(client, SET, key, flags, exp, body)
}

func getResponse(client *MemcachedClient) MCResponse {
	hdrBytes := make([]byte, HDR_LEN)
	bytesRead, err := io.ReadFull(client.Conn, hdrBytes)
	if err != nil || bytesRead != HDR_LEN {
		log.Printf("Error reading message: %s (%d bytes)", err, bytesRead)
		runtime.Goexit()
	}
	res := grokHeader(hdrBytes)
	readContents(client.Conn, res)
	return res
}

func readContents(s net.Conn, res MCResponse) {
	readOb(s, res.Extras)
	readOb(s, res.Key)
	readOb(s, res.Body)
}

func grokHeader(hdrBytes []byte) (rv MCResponse) {
	if hdrBytes[0] != RES_MAGIC {
		log.Printf("Bad magic: %x", hdrBytes[0])
		runtime.Goexit()
	}
	// rv.Opcode = hdrBytes[1]
	rv.Key = make([]byte, ReadUint16(hdrBytes, 2))
	rv.Extras = make([]byte, hdrBytes[4])
	rv.Status = uint16(hdrBytes[7])
	bodyLen := ReadUint32(hdrBytes, 8) - uint32(len(rv.Key)) - uint32(len(rv.Extras))
	rv.Body = make([]byte, bodyLen)
	// rv.Opaque = ReadUint32(hdrBytes, 12)
	rv.Cas = ReadUint64(hdrBytes, 16)
	return
}

func transmitRequest(o *bufio.Writer, req MCRequest) {
	// 0
	writeByte(o, REQ_MAGIC)
	writeByte(o, req.Opcode)
	writeUint16(o, uint16(len(req.Key)))
	// 4
	writeByte(o, uint8(len(req.Extras)))
	writeByte(o, 0)
	writeUint16(o, 0)
	// 8
	writeUint32(o, uint32(len(req.Body))+
		uint32(len(req.Key))+
		uint32(len(req.Extras)))
	// 12
	writeUint32(o, req.Opaque)
	// 16
	writeUint64(o, req.Cas)
	// The rest
	writeBytes(o, req.Extras)
	writeBytes(o, req.Key)
	writeBytes(o, req.Body)
	o.Flush()
}

func writeBytes(s *bufio.Writer, data []byte) {
	if len(data) > 0 {
		written, err := s.Write(data)
		if err != nil || written != len(data) {
			log.Printf("Error writing bytes:  %s", err)
			runtime.Goexit()
		}
	}
	return

}

func writeByte(s *bufio.Writer, b byte) {
	var data []byte = make([]byte, 1)
	data[0] = b
	writeBytes(s, data)
}

func writeUint16(s *bufio.Writer, n uint16) {
	data := WriteUint16(n)
	writeBytes(s, data)
}

func writeUint32(s *bufio.Writer, n uint32) {
	data := WriteUint32(n)
	writeBytes(s, data)
}

func writeUint64(s *bufio.Writer, n uint64) {
	data := WriteUint64(n)
	writeBytes(s, data)
}

func readOb(s net.Conn, buf []byte) {
	x, err := io.ReadFull(s, buf)
	if err != nil || x != len(buf) {
		log.Printf("Error reading part: %s", err)
		runtime.Goexit()
	}
}
