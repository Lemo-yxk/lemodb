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

var db = Open("./example/data")

func BenchmarkStdAppLogs_normal_jsoniter(b *testing.B) {
	//  _ = db.Set("hello", "world")

	// for i := 0; i < 10000; i++ {
	// 	_ = db.Set("hello", "world")
	// }

	// fmt.Println(db.Get("hello").Value())

	for j := 0; j <= b.N; j++ {
		// db.Get(strconv.Itoa(j))
		// db.Set(strconv.Itoa(j), "a")
		// db.Set(strconv.Itoa(j), strconv.Itoa(j))
		// db.Transaction()
		db.Get("c")
		// db.Commit()
	}
}
