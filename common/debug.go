// +build debug

package utils

import LOG "github.com/vinllen/log4go"

func DEBUG_LOG(arg0 interface{}, args ...interface{}) {
	LOG.Debug(arg0, args)
}
