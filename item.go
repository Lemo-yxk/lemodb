/**
* @program: lemodb
*
* @description:
*
* @author: lemo
*
* @create: 2020-06-13 15:32
**/

package lemodb

import "fmt"

type Type byte

const (
	STRING Type = iota
	LIST
)

func (t Type) String() string {
	switch t {
	case 0:
		return "string"
	case 1:
		return "list"
	}
	panic("unknown key type")
}

type Item struct {
	key     []byte
	value   []byte
	keyType Type
	meta    byte
	ttl     uint32
}

func (i *Item) String() string {
	return fmt.Sprintf("Meta: %d, Type: %s, TTL: %d, Key: %s, Value: %s", i.meta, i.keyType, i.ttl, i.key, i.value)
}

func (i *Item) Value() []byte {
	var value = make([]byte, len(i.value))
	copy(value, i.value)
	return value
}

func (i *Item) Key() []byte {
	var key = make([]byte, len(i.key))
	copy(key, i.key)
	return key
}

func (i *Item) TTL() uint32 {
	return i.ttl
}

func (i *Item) Meta() byte {
	return i.meta
}

func (i *Item) Type() Type {
	return i.keyType
}
