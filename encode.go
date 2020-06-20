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
	"fmt"
	"time"
)

// TODO SET
// 0 command
// 1 key len 255
// 2 - 3 value len 65535
// ...key
// ...value

// TODO DEL
// 0 command
// 1 key len 255
// ...key

// TODO TTL
// 0 command
// 1 - 8 ttl value int64
// 9 key len 255
// ...key

// TODO LPUSH
// 0 command
// 1 key len 255
// 2 - 3 value len 65535
// ...key
// ...value

// TODO RPOP
// 0 command
// 1 key len 255
// ...key

// TODO LREM
// 0 command
// 1 - 4 index value int
// 5 key len 255
// ...key

// TODO LSET
// 0 command
// 1 - 4 index value int
// 5 key len 255
// 6 - 7 value len 65535
// ...key
// ...value

// TODO META
// 0 command
// 1 meta
// 2 key len 255
// ...key

// TODO LMETA
// 0 command
// 1 meta
// 2 - 5 index value int
// 6 key len 255
// ...key

// TODO HMETA
// 0 command
// 1 meta
// 2 key len 255
// 3 k len 255
// ...key
// ...k

// TODO HSET
// 0 command
// 1 key len 255
// 2 k len 255
// 3 - 4 v len 65535
// ...key
// ...k
// ...v

// TODO HDEL
// 0 command
// 1 key len 255
// 2 k len 255
// ...key
// ...k

func checkKey(key []byte) error {
	if len(key) > 255 {
		return fmt.Errorf("key len overflow 255")
	}
	return nil
}

func checkValue(value []byte) error {
	if len(value) > 65535 {
		return fmt.Errorf("value len overflow 65535")
	}
	return nil
}

func encodeString(item *base) (message []byte) {
	var str = item.data.(*String)
	var data = encodeSet(item.key, str.data.value)

	var t = time.Now().UnixNano()

	if item.ttl > t {
		data = append(data, encodeTTL(item.key, item.ttl)...)
	}

	if str.data.meta != 0 {
		data = append(data, encodeMeta(str.data.meta, item.key)...)
	}

	return data
}

func encodeList(item *base) (message []byte) {
	var list = item.data.(*List)
	var data []byte
	var t = time.Now().UnixNano()
	for i := len(list.data) - 1; i >= 0; i-- {
		data = append(data, encodeLPush(item.key, list.data[i].value)...)
		if list.data[i].meta != 0 {
			data = append(data, encodeMeta(list.data[i].meta, item.key)...)
		}
	}
	if item.ttl > t {
		data = append(data, encodeTTL(item.key, item.ttl)...)
	}

	return data
}

func encodeHash(item *base) (message []byte) {
	var hash = item.data.(*Hash)
	var data []byte
	var t = time.Now().UnixNano()
	for key, value := range hash.data {
		data = append(data, encodeHSet(item.key, []byte(key), value.value)...)
		if value.meta != 0 {
			data = append(data, encodeHMeta(value.meta, item.key, []byte(key))...)
		}
	}

	if item.ttl > t {
		data = append(data, encodeTTL(item.key, item.ttl)...)
	}

	return data
}

func encodeHSet(key []byte, k []byte, v []byte) (message []byte) {
	var keyL = len(key)
	var kl = len(k)
	var vl = len(v)
	var data = make([]byte, 5+keyL+kl+vl)
	data[0] = byte(HSET)
	data[1] = byte(keyL)
	data[2] = byte(kl)
	binary.LittleEndian.PutUint16(data[3:5], uint16(vl))
	copy(data[5:5+keyL], key)
	copy(data[5+keyL:5+keyL+kl], k)
	copy(data[5+keyL+kl:], v)
	return data
}

func encodeHDel(key []byte, k []byte) (message []byte) {
	var keyL = len(key)
	var kl = len(k)
	var data = make([]byte, 3+keyL+kl)
	data[0] = byte(HDEL)
	data[1] = byte(keyL)
	data[2] = byte(kl)
	copy(data[3:3+keyL], key)
	copy(data[3+keyL:], k)
	return data
}

func encodeHMeta(meta byte, key []byte, k []byte) (message []byte) {
	var keyL = len(key)
	var kl = len(k)
	var data = make([]byte, 4+keyL+kl)
	data[0] = byte(HMETA)
	data[1] = meta
	data[2] = byte(keyL)
	data[3] = byte(kl)
	copy(data[4:4+keyL], key)
	copy(data[4+keyL:], k)
	return data
}

func encodeSet(key []byte, value []byte) (message []byte) {
	var kl = len(key)
	var vl = len(value)
	var data = make([]byte, 4+kl+vl)
	data[0] = byte(SET)
	data[1] = byte(kl)
	binary.LittleEndian.PutUint16(data[2:4], uint16(vl))
	copy(data[4:4+kl], key)
	copy(data[4+kl:], value)
	return data
}

func encodeDel(key []byte) (message []byte) {
	var kl = len(key)
	var data = make([]byte, 2+kl)
	data[0] = byte(DEL)
	data[1] = byte(kl)
	copy(data[2:], key)
	return data
}

func encodeTTL(key []byte, ttl int64) (message []byte) {
	var kl = len(key)
	var data = make([]byte, 10+kl)
	data[0] = byte(TTL)
	binary.LittleEndian.PutUint64(data[1:9], uint64(ttl))
	data[9] = byte(kl)
	copy(data[10:], key)
	return data
}

func encodeLPush(key []byte, value []byte) (message []byte) {
	var kl = len(key)
	var vl = len(value)
	var data = make([]byte, 4+kl+vl)
	data[0] = byte(LPUSH)
	data[1] = byte(kl)
	binary.LittleEndian.PutUint16(data[2:4], uint16(vl))
	copy(data[4:4+kl], key)
	copy(data[4+kl:], value)
	return data
}

func encodeRPop(key []byte) (message []byte) {
	var kl = len(key)
	var data = make([]byte, 2+kl)
	data[0] = byte(RPOP)
	data[1] = byte(kl)
	copy(data[2:], key)
	return data
}

func encodeLRem(key []byte, index int) (message []byte) {
	var kl = len(key)
	var data = make([]byte, 6+kl)
	data[0] = byte(LREM)
	binary.LittleEndian.PutUint32(data[1:5], uint32(index))
	data[5] = byte(kl)
	copy(data[6:], key)
	return data
}

func encodeLSet(key []byte, index int, value []byte) (message []byte) {
	var kl = len(key)
	var vl = len(value)
	var data = make([]byte, 8+kl+vl)
	data[0] = byte(LSET)
	binary.LittleEndian.PutUint32(data[1:5], uint32(index))
	data[5] = byte(kl)
	binary.LittleEndian.PutUint16(data[6:8], uint16(vl))
	copy(data[8:8+kl], key)
	copy(data[8+kl:], value)
	return data
}

func encodeMeta(meta byte, key []byte) (message []byte) {
	var kl = len(key)
	var data = make([]byte, 3+kl)
	data[0] = byte(META)
	data[1] = meta
	data[2] = byte(kl)
	copy(data[3:], key)
	return data
}

func encodeLMeta(meta byte, index int, key []byte) (message []byte) {
	var kl = len(key)
	var data = make([]byte, 7+kl)
	data[0] = byte(META)
	data[1] = meta
	binary.LittleEndian.PutUint32(data[2:6], uint32(index))
	data[6] = byte(kl)
	copy(data[7:], key)
	return data
}

func reader() func(buf []byte, fn func(bytes []byte)) {

	var singleMessageLen = 0

	return func(buf []byte, fn func(bytes []byte)) {
		for {

			// jump out and read continue
			if len(buf) == 0 {
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

	switch Command(message[0]) {
	case SET:
		return 4 + int(message[1]) + int(binary.LittleEndian.Uint16(message[2:4]))
	case DEL:
		return 2 + int(message[1])
	case TTL:
		return 10 + int(message[9])
	case LPUSH:
		return 4 + int(message[1]) + int(binary.LittleEndian.Uint16(message[2:4]))
	case RPOP:
		return 2 + int(message[1])
	case LREM:
		return 6 + int(message[5])
	case LSET:
		return 8 + int(message[5]) + int(binary.LittleEndian.Uint16(message[6:8]))
	case META:
		return 3 + int(message[2])
	case LMETA:
		return 7 + int(message[6])
	case HMETA:
		return 4 + int(message[2]) + int(message[3])
	case HSET:
		return 5 + int(message[1]) + int(message[2]) + int(binary.LittleEndian.Uint16(message[3:5]))
	case HDEL:
		return 3 + int(message[1]) + int(message[2])
	default:
		panic("unknown command")
	}
}

func decodeSet(message []byte) (key []byte, value []byte) {
	var keyLen = message[1]
	return message[4 : 4+keyLen], message[4+keyLen:]
}

func decodeDel(message []byte) (key []byte) {
	return message[2:]
}

func decodeTTL(message []byte) (ttl int64, key []byte) {
	return int64(binary.LittleEndian.Uint64(message[1:10])), message[10:]
}

func decodeLPush(message []byte) (key []byte, value []byte) {
	var keyLen = message[1]
	return message[4 : 4+keyLen], message[4+keyLen:]
}

func decodeRPop(message []byte) (key []byte) {
	return message[2:]
}

func decodeLRem(message []byte) (index int, key []byte) {
	return int(binary.LittleEndian.Uint32(message[1:5])), message[6:]
}

func decodeLSet(message []byte) (index int, key []byte, value []byte) {
	var keyLen = message[5]
	return int(binary.LittleEndian.Uint32(message[1:5])), message[8 : 8+keyLen], message[8+keyLen:]
}

func decodeMeta(message []byte) (meta byte, key []byte) {
	return message[1], message[3:]
}

func decodeLMeta(message []byte) (meta byte, index int, key []byte) {
	return message[1], int(binary.LittleEndian.Uint32(message[2:6])), message[6:]
}

func decodeHMeta(message []byte) (meta byte, key []byte, k []byte) {
	var keyLen = message[2]
	return message[1], message[4 : 4+keyLen], message[4+keyLen:]
}

func decodeHSet(message []byte) (key []byte, k []byte, v []byte) {
	var keyLen = message[1]
	var kl = message[2]
	return message[5 : 5+keyLen], message[5+keyLen : 5+keyLen+kl], message[5+keyLen+kl:]
}

func decodeHDel(message []byte) (key []byte, k []byte) {
	var keyLen = message[1]
	return message[3 : 3+keyLen], message[3+keyLen:]
}

type Command byte

const (
	SET Command = iota
	DEL
	TTL
	LPUSH
	RPOP
	LREM
	LSET
	META
	LMETA
	HMETA
	HSET
	HDEL
)
