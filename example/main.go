/**
* @program: lemodb
*
* @description:
*
* @author: lemo
*
* @create: 2020-06-13 14:23
**/

package main

import (
	"log"

	"github.com/Lemo-yxk/lemodb"
)

func init() {

}

func main() {
	var db = lemodb.Open(&lemodb.Option{Path: "./example/data"})

	// db.Set("hello", []byte("world"))

	// db.SetEntry(lemodb.NewEntry("hello",[]byte("world")).WithTTL(1))
	// db.Del("hello")
	// db.LPush("a", []byte("a"), []byte("b"))
	// db.LPush("a", []byte("c"), []byte("d"))
	// db.LPush("a","d","e")

	// for i := 0; i < 100; i++ {
	// 	db.Set(strconv.Itoa(i), strings.Repeat("world", 100))
	// }
	// fmt.Println(db.Get(strconv.Itoa(0)).Value())
	// for i := 0; i < 10000; i++ {
	// 	fmt.Println(db.Get(strconv.Itoa(0)).Value())
	// }

	// var items = db.List("a")
	// for i := 0; i < len(items); i++ {
	// 	log.Println(items[i])
	// }

	// db.LPushEntry(lemodb.NewEntry("a",[]byte("a")).WithTTL(5))
	// db.LPushEntry(lemodb.NewEntry("a",[]byte("b")).WithTTL(10))
	// db.LPushEntry(lemodb.NewEntry("a",[]byte("c")).WithTTL(15))
	// // log.Println(db.Count())
	// log.Println(db.RPop("a"))

	// for j := 0; j <= 10000; j++ {
	// 	// db.Get(strconv.Itoa(j))
	// 	go db.Set(strconv.Itoa(j), []byte("a"))
	// }

	// log.Println(db.Index())
	// db.LPush("a", "1", "2")
	// db.Meta("400000", 1)
	// db.Del("hello-999999")
	// log.Println(db.Get("hello-499999").Value())
	// db.Set("hello", "world")
	// db.TTL("hello", 8*time.Second)
	// time.Sleep(5 * time.Second)

	// for i := 0; i < 2000000; i++ {
	// 	// var key = fmt.Sprintf("hello-%d",i)
	// 	db.Set(strconv.Itoa(i), strings.Repeat("a", 1024))
	// }

	// db.LPush("hello","world")
	// db.LRem("hello",1)
	// db.LSet("hello",4,"3")
	// log.Println(db.List("hello").Value())
	// log.Println(db.RPop("a"))

	// db.HSet("a", "1", "2")
	// db.HMeta(0, "a", "1")

	// res,_ := db.List("b")
	// res.Data()[0] = nil
	//
	// log.Println(db.List("b"))

	// db.DropAll()
	//
	// db.LPush("a", "1", "2")
	//
	// var res, _ = db.List("a")
	// var dres []string
	// res.Range(func(value string) {
	// 	dres = append(dres, value)
	// })
	//
	// for i := 0; i < len(dres); i++ {
	// 	db.LRemV("a", dres[i])
	// }
	//
	// log.Println(db.List("a"))
	//
	// db.Keys(func(tp lemodb.Type, ttl int64, key string) bool {
	// 	log.Println(tp, ttl, key)
	// 	return true
	// })

	// db.DropAll()
	//
	// db.Transaction()
	//
	// _ = db.LPush("a", "1", "2", "3")
	//
	// log.Println(db.List("a"))
	//
	// db.Commit()
	//
	// log.Println(db.List("a"))
	//
	// db.Transaction()
	//
	// _ = db.LPush("b", "1", "2", "3")
	//
	// log.Println(db.List("b"))
	//
	// db.Commit()
	//
	// log.Println(db.List("b"))

	log.Println(db.Count())

	_ = db
}
