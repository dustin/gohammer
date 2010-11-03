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

type MemcachedClient struct {
	Conn net.Conn
}

func Connect(prot string, dest string) (rv MemcachedClient) {
	conn, err := net.Dial(prot, "", dest)
        if err != nil {
		log.Exitf("Failed to connect: %s", err)
	}
	rv.Conn = conn
	return rv
}

func send(client MemcachedClient, req MCRequest) (rv MCResponse) {
	transmitRequest(client.Conn, req)
	rv = getResponse(client)
	return
}

func Get(client MemcachedClient, key string) MCResponse {
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

func Del(client MemcachedClient, key string) MCResponse {
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

func store(client MemcachedClient, opcode uint8,
	key string, body string) MCResponse {

	var req MCRequest
	req.Opcode = opcode
	req.Cas = 0
	req.Opaque = 0
	req.Key = make([]byte, len(key))
	copy(req.Key, key)
	// This is actually just two 32 bit numbers.  I don't care
	// about them, though.
	req.Extras = WriteUint64(0)
	req.Body = make([]byte, len(body))
	return send(client, req)
}

func Add(client MemcachedClient, key string, body string) MCResponse {
	return store(client, ADD, key, body)
}

func getResponse(client MemcachedClient) MCResponse {
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

/*
     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| Magic         | Opcode        | Key Length                    |
       +---------------+---------------+---------------+---------------+
      4| Extras length | Data type     | Status                        |
       +---------------+---------------+---------------+---------------+
      8| Total body length                                             |
       +---------------+---------------+---------------+---------------+
     12| Opaque                                                        |
       +---------------+---------------+---------------+---------------+
     16| CAS                                                           |
       |                                                               |
       +---------------+---------------+---------------+---------------+
       Total 24 bytes
*/

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

/*
     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| Magic         | Opcode        | Key length                    |
       +---------------+---------------+---------------+---------------+
      4| Extras length | Data type     | Reserved                      |
       +---------------+---------------+---------------+---------------+
      8| Total body length                                             |
       +---------------+---------------+---------------+---------------+
     12| Opaque                                                        |
       +---------------+---------------+---------------+---------------+
     16| CAS                                                           |
       |                                                               |
       +---------------+---------------+---------------+---------------+
       Total 24 bytes


 >28 Read binary protocol data:
 >28   0x80 0x00 0x00 0x01
 >28   0x08 0x00 0x00 0x00
 >28   0x00 0x00 0x00 0x0b
 >28   0x00 0x00 0x00 0x00
 >28   0x00 0x00 0x00 0x00
 >28   0x00 0x00 0x00 0x00

        msg=struct.pack(REQ_PKT_FMT, REQ_MAGIC_BYTE,
            cmd, len(key), len(extraHeader), dtype, self.vbucketId,
                len(key) + len(extraHeader) + len(val), opaque, cas)

 # magic, opcode, keylen, extralen, datatype, vbucket, bodylen, opaque, cas
 REQ_PKT_FMT=">BBHBBHIIQ"

*/

func transmitRequest(s net.Conn, req MCRequest) {
	o := bufio.NewWriter(s)
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
