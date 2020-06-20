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
	"testing"
)

func BenchmarkStdAppLogs_normal_jsoniter(b *testing.B) {
	//  _ = db.Set("hello", "world")

	// for i := 0; i < 10000; i++ {
	// 	_ = db.Set("hello", "world")
	// }

	// fmt.Println(db.Get("hello").Value())
	var db = Open("./example/data")

	for j := 0; j <= b.N; j++ {
		// db.Get(strconv.Itoa(j))
		// db.Set(strconv.Itoa(j), "a")
		// db.Set(strconv.Itoa(j), strconv.Itoa(j))
		db.Set("c", "1")
	}

	db.Close()
}
