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
	var db = lemodb.Open("./example/data")
	db.Close()
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
	db.Del("a")

	// for j := 0; j <= 10000; j++ {
	// 	// db.Get(strconv.Itoa(j))
	// 	go db.Set(strconv.Itoa(j), []byte("a"))
	// }

	log.Println(db.Index())

	// time.Sleep(5 * time.Second)
	// _ = db
}
