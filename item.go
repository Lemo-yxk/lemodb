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

type val struct {
	value []byte
	meta  byte
}

type String struct {
	data *val
}

type List struct {
	data []*val
}

type Hash struct {
	data map[string]*val
}

func (v *val) Meta() byte {
	return v.meta
}

func (v *val) Value() string {
	return string(v.value)
}

func (s *String) Meta() byte {
	return s.Meta()
}

func (s *String) Value() string {
	return s.Value()
}

func (l *List) Range(fn func(val *val)) {
	for i := 0; i < len(l.data); i++ {
		fn(l.data[i])
	}
}

func (l *List) Len() int {
	return len(l.data)
}

func (h *Hash) Range(fn func(key string, val *val)) {
	for key, val := range h.data {
		fn(key, val)
	}
}

func (h *Hash) Len() int {
	return len(h.data)
}
