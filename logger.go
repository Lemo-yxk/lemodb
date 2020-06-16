/**
* @program: lemodb
*
* @description:
*
* @author: lemo
*
* @create: 2020-06-13 14:17
**/

package lemodb

import (
	"fmt"
	"time"
)

type Logger interface {
	Infof(format string, args ...interface{})
	Debugf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Warningf(format string, args ...interface{})
}

type defaultLogger struct{}

func (logger *defaultLogger) Infof(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

func (logger *defaultLogger) Debugf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

func (logger *defaultLogger) Errorf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

func (logger *defaultLogger) Warningf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

var log Logger

func Infof(format string, args ...interface{}) {
	if log == nil {
		return
	}
	args = append([]interface{}{time.Now().Format("2006-01-02 15:04:05")}, args...)
	log.Infof("INF %s "+format, args...)
}

func Debugf(format string, args ...interface{}) {
	if log == nil {
		return
	}
	args = append([]interface{}{time.Now().Format("2006-01-02 15:04:05")}, args...)
	log.Debugf("INF %s "+format, args...)
}

func Errorf(format string, args ...interface{}) {
	if log == nil {
		return
	}
	args = append([]interface{}{time.Now().Format("2006-01-02 15:04:05")}, args...)
	log.Errorf("INF %s "+format, args...)
}

func Warningf(format string, args ...interface{}) {
	if log == nil {
		return
	}
	args = append([]interface{}{time.Now().Format("2006-01-02 15:04:05")}, args...)
	log.Warningf("INF %s "+format, args...)
}
