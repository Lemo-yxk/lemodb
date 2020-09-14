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

func Md5(input []byte) string {
	var byte16 = md5.Sum(input)
	var bytes = make([]byte, 16)
	for i := 0; i < 16; i++ {
		bytes[i] = byte16[i]
	}
	return hex.EncodeToString(bytes)
}

func Benchmark_Set_Number(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var key = strconv.Itoa(i)
		err := db.Set(key, key)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Get_Number(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := db.Get("1")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Hash_HSet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var key = strconv.Itoa(i) + "hash"
		err := db.HSet(key, key, key)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Hash_HGet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := db.HGet("1hash", "1hash")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Hash_HGetAll(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := db.HGetAll("1hash")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_List_Push(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var key = strconv.Itoa(i) + "list"
		err := db.LPush(key, key)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_List_RPop(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var key = strconv.Itoa(i) + "list"
		_, _ = db.RPop(key)
	}
}
