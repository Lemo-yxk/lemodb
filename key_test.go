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

func assert(t *testing.T, condition bool, args ...interface{}) {
	if condition {
		t.Fatal(args...)
	}
}

func getList(list *List) string {
	var s = ""
	list.Range(func(value string) {
		s += value
	})
	return s
}

func TestString(t *testing.T) {
	var db = Open("./example/data")

	// DropAll
	db.DropAll()
	assert(t, db.Count() != 0, "keys not 0")

	// String
	// get
	_, err := db.Get("a")
	assert(t, err == nil, "string not empty")
	// set
	err = db.Set("a", "1")
	assert(t, err != nil, "string set err", err)
	str, err := db.Get("a")
	assert(t, err != nil, "string get err", err)
	assert(t, str.Value() != "1", "string not eq 1")
	// ttl
	err = db.Expired("a", time.Second)
	assert(t, err != nil, "string expired err", err)
	ttl, err := db.TTL("a")
	assert(t, err != nil, "string ttl err", err)
	assert(t, ttl.Milliseconds() > time.Second.Milliseconds(), "string ttl time err", err)
	// del
	err = db.Del("a")
	assert(t, err != nil, "string del err", err)

	// List
	// lpush
	err = db.LPush("a")
	assert(t, err == nil, "list lpush err", err)
	err = db.LPush("a", "1", "2", "3")
	list, err := db.List("a")
	assert(t, err != nil, "list get err", err)
	assert(t, list.Len() != 3, "list push count err", err)
	assert(t, getList(list) != "321", "list item err", err)
	// lrem
	val, err := db.LRem("a", 0)
	assert(t, err != nil, "list lrem err", err)
	assert(t, val != "3", "list lrem val err", err)
	assert(t, list.Len() != 2, "list lrem count err", err)
	assert(t, getList(list) != "21", "list item err", err)
	// lset
	err = db.LSet("a", 1, "9")
	assert(t, err != nil, "list lset err", err)
	assert(t, list.Len() != 3, "list lset count err", err)
	assert(t, getList(list) != "291", "list item err", err)
	// rpop
	val, err = db.RPop("a")
	assert(t, err != nil, "list rpop err", err)
	assert(t, val != "1", "list rpop val err", err)
	// ttl
	err = db.Expired("a", time.Second)
	assert(t, err != nil, "list expired err", err)
	ttl, err = db.TTL("a")
	assert(t, err != nil, "list ttl err", err)
	assert(t, ttl.Milliseconds() > time.Second.Milliseconds(), "list ttl time err", err)
	// del
	err = db.Del("a")
	assert(t, err != nil, "list del err", err)

	// Hash
	// hset
	err = db.HSet("a", "1", "2")
	assert(t, err != nil, "hash hset err", err)
	err = db.HSet("a", "3", "4")
	assert(t, err != nil, "hash hset err", err)
	hash, err := db.HGetAll("a")
	assert(t, err != nil, "hash hget all err", err)
	assert(t, hash.Len() != 2, "hash hget all count err", err)
	// hget
	val, err = db.HGet("a", "1")
	assert(t, err != nil, "hash hget err", err)
	assert(t, val != "2", "hash hget val err", err)
	val, err = db.HGet("a", "3")
	assert(t, err != nil, "hash hget err", err)
	assert(t, val != "4", "hash hget val err", err)
	// hdel
	err = db.HDel("a", "1")
	assert(t, err != nil, "hash hdel err", err)
	assert(t, hash.Len() != 1, "hash hdel count err", err)
	_, err = db.HGet("a", "1")
	assert(t, err == nil, "hash hdel val err", err)
	// ttl
	err = db.Expired("a", time.Second)
	assert(t, err != nil, "hash expired err", err)
	ttl, err = db.TTL("a")
	assert(t, err != nil, "hash ttl err", err)
	assert(t, ttl.Milliseconds() > time.Second.Milliseconds(), "hash ttl time err", err)
	// del
	err = db.Del("a")
	assert(t, err != nil, "hash del err", err)

	// transaction
	db.Transaction()
	err = db.Set("a", "1")
	assert(t, err != nil, "transaction set err", err)
	str, err = db.Get("a")
	assert(t, err == nil, "transaction get err", err)
	assert(t, str != nil, "transaction get val err", err)
	db.Commit()
	str, err = db.Get("a")
	assert(t, err != nil, "transaction get err", err)
	assert(t, str.Value() != "1", "transaction get val err", err)

	// index
	assert(t, db.Index() != 17, "index err", err)

	// close
	db.Close()
}
