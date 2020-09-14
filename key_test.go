/**
* @program: lemodb
*
* @description:
*
* @author: lemo
*
* @create: 2020-06-20 19:32
**/

package lemodb

import (
	"testing"
	"time"
)

func newAssertEqual(t *testing.T) func(condition bool, args ...interface{}) {
	return func(condition bool, args ...interface{}) {
		assertEqual(t, condition, args...)
	}
}

func assertEqual(t *testing.T, condition bool, args ...interface{}) {
	if condition {
		t.Fatal(args...)
	}
}

func listToString(list *List) string {
	var s = ""
	list.Range(func(value string) {
		s += value
	})
	return s
}

var db *DB

func TestMain(t *testing.M) {
	db = Open(&Option{Path: "./example/data"})
	// DropAll
	db.DropAll()
	t.Run()
}

func Test_DB_Count(t *testing.T) {
	assert := newAssertEqual(t)
	assert(db.Count() != 0, "keys not 0")
}

func Test_String(t *testing.T) {
	assert := newAssertEqual(t)

	// get
	_, err := db.Get("a")
	assert(err == nil, "string not empty")

	// set
	err = db.Set("a", "1")
	assert(err != nil, "string set err", err)
	str, err := db.Get("a")
	assert(err != nil, "string get err", err)
	assert(str.Value() != "1", "string not eq 1")

	// TTL
	err = db.Expired("a", time.Second)
	assert(err != nil, "string expired err", err)
	ttl, err := db.TTL("a")
	assert(err != nil, "string ttl err", err)
	assert(ttl.Milliseconds() > time.Second.Milliseconds(), "string ttl time err", err)

	// DEL
	err = db.Del("a")
	assert(err != nil, "string del err", err)
}

func Test_List(t *testing.T) {
	assert := newAssertEqual(t)

	// lpush
	err := db.LPush("a")
	assert(err == nil, "list lpush err", err)
	err = db.LPush("a", "1", "2", "3")
	list, err := db.List("a")
	assert(err != nil, "list get err", err)
	assert(list.Len() != 3, "list push count err", err)
	assert(listToString(list) != "321", "list item err", err)

	// lrem
	val, err := db.LRem("a", 0)
	assert(err != nil, "list lrem err", err)
	assert(val != "3", "list lrem val err", err)
	assert(list.Len() != 2, "list lrem count err", err)
	assert(listToString(list) != "21", "list item err", err)

	// lset
	err = db.LSet("a", 1, "9")
	assert(err != nil, "list lset err", err)
	assert(list.Len() != 3, "list lset count err", err)
	assert(listToString(list) != "291", "list item err", err)

	// rpop
	val, err = db.RPop("a")
	assert(err != nil, "list rpop err", err)
	assert(val != "1", "list rpop val err", err)

	// ttl
	err = db.Expired("a", time.Second)
	assert(err != nil, "list expired err", err)
	ttl, err := db.TTL("a")
	assert(err != nil, "list ttl err", err)
	assert(ttl.Milliseconds() > time.Second.Milliseconds(), "list ttl time err", err)

	// del
	err = db.Del("a")
	assert(err != nil, "list del err", err)
}

func Test_Hash(t *testing.T) {
	assert := newAssertEqual(t)

	// hset
	err := db.HSet("a", "1", "2")
	assert(err != nil, "hash hset err", err)
	err = db.HSet("a", "3", "4")
	assert(err != nil, "hash hset err", err)
	hash, err := db.HGetAll("a")
	assert(err != nil, "hash hget all err", err)
	assert(hash.Len() != 2, "hash hget all count err", err)

	// hget
	val, err := db.HGet("a", "1")
	assert(err != nil, "hash hget err", err)
	assert(val != "2", "hash hget val err", err)
	val, err = db.HGet("a", "3")
	assert(err != nil, "hash hget err", err)
	assert(val != "4", "hash hget val err", err)

	// hdel
	err = db.HDel("a", "1")
	assert(err != nil, "hash hdel err", err)
	assert(hash.Len() != 1, "hash hdel count err", err)
	_, err = db.HGet("a", "1")
	assert(err == nil, "hash hdel val err", err)

	// ttl
	err = db.Expired("a", time.Second)
	assert(err != nil, "hash expired err", err)
	ttl, err := db.TTL("a")
	assert(err != nil, "hash ttl err", err)
	assert(ttl.Milliseconds() > time.Second.Milliseconds(), "hash ttl time err", err)

	// del
	err = db.Del("a")
	assert(err != nil, "hash del err", err)
}

func Test_Transaction(t *testing.T) {
	assert := newAssertEqual(t)

	// transaction
	db.Transaction()
	err := db.Set("a", "1")
	assert(err != nil, "transaction set err", err)
	db.CleanDelayCommit()
	str, err := db.Get("a")
	assert(err == nil, "transaction get err", err)
	err = db.Set("a", "1")
	assert(err != nil, "transaction set err", err)
	str, err = db.Get("a")
	assert(err == nil, "transaction get err", err)
	assert(str != nil, "transaction get val err", err)
	db.Commit()
	str, err = db.Get("a")
	assert(err != nil, "transaction get err", err)
	assert(str.Value() != "1", "transaction get val err", err)
}

func Test_Index(t *testing.T) {
	assert := newAssertEqual(t)

	assert(db.Index() != 17, "index err")
}
