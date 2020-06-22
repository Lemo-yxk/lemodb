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

type String struct {
	data []byte
}

type List struct {
	data [][]byte
}

type Hash struct {
	data map[string][]byte
}

func (s *String) Value() string {
	return string(s.data)
}

func (l *List) Range(fn func(value string)) {
	for i := 0; i < len(l.data); i++ {
		fn(string(l.data[i]))
	}
}

func (l *List) Len() int {
	return len(l.data)
}

func (h *Hash) Range(fn func(key string, value string)) {
	for key, val := range h.data {
		fn(key, string(val))
	}
}

func (h *Hash) Len() int {
	return len(h.data)
}
