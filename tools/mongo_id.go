package main

import (
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage", os.Args[0], "${ObjectId}")
		os.Exit(0)
	}

	oid := bson.ObjectIdHex(os.Args[1])
	fmt.Println("timestamp :", oid.Time().Unix(), ",", time.Unix(oid.Time().Unix(), 0).Format("2006-01-02 15:04:05"))
	fmt.Println("machine :", oid.Machine())
	fmt.Println("pid", oid.Pid())
	fmt.Println("counter", oid.Counter())

}
