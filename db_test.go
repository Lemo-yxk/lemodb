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
	"crypto/md5"
	"encoding/hex"
	"strconv"
	"testing"
)

var db = Open(&Option{Path: "./example/data"})

func Md5(input []byte) string {
	var byte16 = md5.Sum(input)
	var bytes = make([]byte, 16)
	for i := 0; i < 16; i++ {
		bytes[i] = byte16[i]
	}
	return hex.EncodeToString(bytes)
}

func BenchmarkStdAppLogs_normal_jsoniter(b *testing.B) {
	//  _ = db.Set("hello", "world")

	// for i := 0; i < 10000; i++ {
	// 	_ = db.Set("hello", "world")
	// }

	// fmt.Println(db.Get("hello").Value())

	for j := 0; j <= b.N; j++ {
		// db.Get(strconv.Itoa(j))
		// db.Set(strconv.Itoa(j), "a")
		db.Set(strconv.Itoa(j), strconv.Itoa(j))
		// db.Transaction()
		// db.Get("0")
		// db.Commit()
	}
}
