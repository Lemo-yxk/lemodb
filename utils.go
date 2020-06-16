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

import "unsafe"

func panicIfNotNil(v ...interface{}) {
	if !isNil(v[len(v)-1]) {
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
