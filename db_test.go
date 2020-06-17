/**
* @program: lemo
*
* @description:
*
* @author: lemo
*
* @create: 2019-10-05 14:19
**/

package lemodb

import (
	log2 "log"
	"strconv"
	"testing"
)

var index = 0

func BenchmarkStdAppLogs_normal_jsoniter(b *testing.B) {
	var db = Open("./example/data")
	//  _ = db.Set("hello", "world")

	// for i := 0; i < 10000; i++ {
	// 	_ = db.Set("hello", "world")
	// }

	// fmt.Println(db.Get("hello").Value())

	for j := 0; j <= b.N; j++ {
		index++
		// db.Get(strconv.Itoa(j))
		db.Set(strconv.Itoa(j), []byte("a"))
	}

	log2.Println("db index:", db.index)

	log2.Println("index:", index)
	db.Close()

}
