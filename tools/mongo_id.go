package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"time"
)

// Copy From bson.go(gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22)
type ObjectId string

func ObjectIdHex(s string) ObjectId {
	d, err := hex.DecodeString(s)
	if err != nil || len(d) != 12 {
		panic(fmt.Sprintf("invalid input to ObjectIdHex: %q", s))
	}
	return ObjectId(d)
}
func (id ObjectId) Time() time.Time {
	// First 4 bytes of ObjectId is 32-bit big-endian seconds from epoch.
	secs := int64(binary.BigEndian.Uint32(id.byteSlice(0, 4)))
	return time.Unix(secs, 0)
}
func (id ObjectId) Machine() []byte {
	return id.byteSlice(4, 7)
}
func (id ObjectId) Pid() uint16 {
	return binary.BigEndian.Uint16(id.byteSlice(7, 9))
}
func (id ObjectId) Counter() int32 {
	b := id.byteSlice(9, 12)
	// Counter is stored as big-endian 3-byte value
	return int32(uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2]))
}
func (id ObjectId) byteSlice(start, end int) []byte {
	if len(id) != 12 {
		panic(fmt.Sprintf("invalid ObjectId: %q", string(id)))
	}
	return []byte(string(id)[start:end])
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage", os.Args[0], "${ObjectId}")
		os.Exit(0)
	}

	oid := ObjectIdHex(os.Args[1])
	fmt.Println("timestamp :", oid.Time().Unix(), ",", time.Unix(oid.Time().Unix(), 0).Format("2006-01-02 15:04:05"))
	fmt.Println("machine :", oid.Machine())
	fmt.Println("pid", oid.Pid())
	fmt.Println("counter", oid.Counter())

}
