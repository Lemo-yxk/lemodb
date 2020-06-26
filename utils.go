/**
* @program: lemodb
*
* @description:
*
* @author: lemo
*
* @create: 2020-06-13 15:14
**/

package lemodb

import (
	"os"
	"unsafe"
)

func panicIfNotNil(v ...interface{}) {
	if !isNil(v[len(v)-1]) {

		defer func() {
			if err := recover(); err != nil {
				log.Errorf("%v", err)
				os.Exit(0)
			}
		}()

		panic(v[len(v)-1])
	}
}

func isNil(i interface{}) bool {
	if i == nil {
		return true
	}
	return (*eFace)(unsafe.Pointer(&i)).data == nil
}

type eFace struct {
	_type unsafe.Pointer
	data  unsafe.Pointer
}

func str2bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

func bytes2str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
