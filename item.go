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

type Type byte

const (
	STRING Type = iota
	LIST
	HASH
)

func (t Type) String() string {
	switch t {
	case STRING:
		return "string"
	case LIST:
		return "list"
	case HASH:
		return "hash"
	}
	panic("unknown key type")
}

type base struct {
	key  []byte
	ttl  int64
	tp   Type
	data interface{}
}

type Val struct {
	value []byte
	meta  byte
}

type String struct {
	data *Val
}

type List struct {
	data []*Val
}

type Hash struct {
	data map[string]*Val
}

func (v *Val) Meta() byte {
	return v.meta
}

func (v *Val) Value() string {
	return string(v.value)
}

func (s *String) Meta() byte {
	return s.data.Meta()
}

func (s *String) Value() string {
	return s.data.Value()
}

func (l *List) Range(fn func(val *Val)) {
	for i := 0; i < len(l.data); i++ {
		fn(l.data[i])
	}
}

func (l *List) Len() int {
	return len(l.data)
}

func (h *Hash) Range(fn func(key string, val *Val)) {
	for key, val := range h.data {
		fn(key, val)
	}
}

func (h *Hash) Len() int {
	return len(h.data)
}
