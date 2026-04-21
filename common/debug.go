//go:build debug
// +build debug

package utils

import l "github.com/alibaba/MongoShake/v2/pkg/log"

func DEBUG_LOG(arg0 interface{}, args ...interface{}) {
	if format, ok := arg0.(string); ok {
		l.Logger.Debugf(format, args...)
		return
	}
	l.Logger.Debugf("%v", arg0)
}
