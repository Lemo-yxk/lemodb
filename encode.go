/**
* @program: lemodb
*
* @description:
*
* @author: lemo
*
* @create: 2020-06-13 16:03
**/

package lemodb

import (
	"encoding/binary"
)

// 0 type

// 1 meta

// 2 ttl
// 3 ttl
// 4 ttl
// 5 ttl

// 6 key len
// 7 key len

// 8 value len
// 9 value len
// 10 value len
// 11 value len

func getKeyType(message []byte) byte {
	return message[0]
}

func getMeta(message []byte) byte {
	return message[1]
}

func getTTL(message []byte) uint32 {
	return binary.LittleEndian.Uint32(message[2:6])
}

func getKey(message []byte) []byte {
	var keyLen = binary.LittleEndian.Uint16(message[6:8])
	return message[12 : 12+keyLen]
}

func getValue(message []byte) []byte {
	var keyLen = binary.LittleEndian.Uint16(message[6:8])
	return message[12+keyLen:]
}

func decode(message []byte) (keyType byte, meta byte, ttl uint32, key []byte, value []byte) {
	var keyLen = binary.LittleEndian.Uint16(message[6:8])
	// var valueLen = binary.LittleEndian.Uint32(message[8:12])
	return message[0], message[1], binary.LittleEndian.Uint32(message[2:6]), message[12 : 12+keyLen], message[12+keyLen:]
}

func encode(raw []byte, keyType byte, meta byte, ttl uint32, key []byte, value []byte) (message []byte) {
	var data []byte
	if raw == nil {
		data = make([]byte, 12)
	} else {
		data = raw
	}
	var kl = len(key)
	var vl = len(value)
	data[0] = keyType
	data[1] = meta
	binary.LittleEndian.PutUint32(data[2:6], ttl)
	binary.LittleEndian.PutUint16(data[6:8], uint16(kl))
	binary.LittleEndian.PutUint32(data[8:12], uint32(vl))
	if raw == nil {
		data = append(data, key...)
		data = append(data, value...)
	} else {
		copy(data[12:12+kl], key)
		copy(data[12+kl:], value)
		var dl = len(data[12+kl:])
		if dl > vl {
			data = data[:len(data)-dl+vl]
		} else if dl < vl {
			data = append(data, value[dl:]...)
		}
	}
	return data
}

func setTTL(data []byte, ttl uint32) {
	binary.LittleEndian.PutUint32(data[2:6], ttl)
}

func reader() func(buf []byte, fn func(bytes []byte)) {

	var singleMessageLen = 0

	return func(buf []byte, fn func(bytes []byte)) {
		for {

			// jump out and read continue
			if len(buf) < 12 {
				return
			}

			// just begin
			if singleMessageLen == 0 {
				singleMessageLen = getLen(buf)
			}

			// a complete message
			fn(buf[0:singleMessageLen])

			// delete this message
			buf = buf[singleMessageLen:]

			// reset len
			singleMessageLen = 0
		}

	}
}

func getLen(message []byte) int {
	// key
	var key = binary.LittleEndian.Uint16(message[6:8])
	var value = binary.LittleEndian.Uint32(message[8:12])
	return 12 + int(key) + int(value)
}
