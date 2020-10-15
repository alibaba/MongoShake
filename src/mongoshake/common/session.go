package utils

import (
	"fmt"
	"strings"
)

const (
	OplogNS                      = "oplog.rs"
	ReadWriteConcernDefault      = ""
	ReadWriteConcernLocal        = "local"
	ReadWriteConcernAvailable    = "available" // for >= 3.6
	ReadWriteConcernMajority     = "majority"
	ReadWriteConcernLinearizable = "linearizable"
)

type NS struct {
	Database   string
	Collection string
}

func (ns NS) Str() string {
	return fmt.Sprintf("%s.%s", ns.Database, ns.Collection)
}

func NewNS(namespace string) NS {
	pair := strings.SplitN(namespace, ".", 2)
	return NS{Database: pair[0], Collection: pair[1]}
}
