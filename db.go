/**
* @program: lemodb
*
* @description:
*
* @author: lemo
*
* @create: 2020-06-13 14:15
**/

package lemodb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

type DB struct {
	option    *Option
	binLog    *os.File
	binData   *os.File
	indexFile *os.File
	index     uint64
	mux       sync.RWMutex
	comMux    sync.RWMutex
	once      sync.Once

	isTranRunning bool
	tranMux       sync.RWMutex
	binTran       *bytes.Buffer

	data []map[string]*base
}

var binDataPath = ""
var binLogPath = ""
var binDataCopyPath = ""
var binLogCopyPath = ""
var indexPath = ""

var NumberCpu = runtime.NumCPU()

func (db *DB) newDataMap() {
	db.data = db.data[:0]
	for i := 0; i < NumberCpu; i++ {
		db.data = append(db.data, make(map[string]*base))
	}
}

func (db *DB) getDataMap(key []byte) map[string]*base {
	return db.data[key[0]%byte(NumberCpu)]
}

func (db *DB) Start() {
	db.once.Do(func() {

		if db.option == nil {
			panic("option is nil")
		}

		if db.option.Path == "" {
			panic("path is nil")
		}

		var t = time.Now()

		var absPath, err = filepath.Abs(db.option.Path)
		panicIfNotNil(err)

		db.option.Path = absPath

		db.newDataMap()

		binDataPath = path.Join(db.option.Path, "bindata")
		binLogPath = path.Join(db.option.Path, "binlog")
		indexPath = path.Join(db.option.Path, "index")
		binLogCopyPath = path.Join(db.option.Path, "binlogcopy")
		binDataCopyPath = path.Join(db.option.Path, "bindatacopy")

		db.checkFile()

		db.openFile()

		db.load(db.binData, false)

		db.load(db.binLog, true)

		db.compressed()

		db.binTran = new(bytes.Buffer)

		fmt.Printf("start success in %d ms\n", time.Now().Sub(t).Milliseconds())
		fmt.Printf("keys: %d\n", db.Count())
	})
}

func (db *DB) checkFile() {

	_, err := os.Stat(binLogCopyPath)
	if err == nil {
		// exists
		var binLogCopyFile = db.openBinLogCopy()
		db.openBinLog()
		panicIfNotNil(io.Copy(binLogCopyFile, db.binLog))
		_ = db.closeBinLog()
		_ = binLogCopyFile.Close()
		panicIfNotNil(os.Remove(binLogPath))
		panicIfNotNil(os.Rename(binLogCopyPath, binLogPath))
	}

	_, err = os.Stat(binDataCopyPath)
	if err == nil {
		// exists
		var binDataCopyFile = db.openBinDataCopy()
		db.openBinData()
		panicIfNotNil(io.Copy(binDataCopyFile, db.binData))
		_ = db.closeBinData()
		_ = binDataCopyFile.Close()
		panicIfNotNil(os.Remove(binDataPath))
		panicIfNotNil(os.Rename(binDataCopyPath, binDataPath))
	}
}

func (db *DB) Close() {
	db.mux.Lock()
	defer db.mux.Unlock()
	_ = db.indexFile.Sync()
	_ = db.binLog.Sync()
	_ = db.binData.Sync()

	_ = db.indexFile.Close()
	_ = db.binLog.Close()
	_ = db.binData.Close()
}

func (db *DB) openFile() {
	db.mux.Lock()
	defer db.mux.Unlock()
	db.openBinLog()
	db.openBinData()
	db.openIndex()
}

func (db *DB) exists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}

	if os.IsNotExist(err) {
		return false
	}

	panic(err)
}

func (db *DB) openIndex() {
	if !db.exists(db.option.Path) {
		panicIfNotNil(os.MkdirAll(db.option.Path, 0755))
	}
	f, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0666)
	panicIfNotNil(err)
	db.indexFile = f
}

func (db *DB) closeIndex() error {
	return db.indexFile.Close()
}

func (db *DB) openBinLog() {
	if !db.exists(db.option.Path) {
		panicIfNotNil(os.MkdirAll(db.option.Path, 0755))
	}
	f, err := os.OpenFile(binLogPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	panicIfNotNil(err)
	db.binLog = f
}

func (db *DB) closeBinLog() error {
	return db.binLog.Close()
}

func (db *DB) openBinData() {
	if !db.exists(db.option.Path) {
		panicIfNotNil(os.MkdirAll(db.option.Path, 0755))
	}
	f, err := os.OpenFile(binDataPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	panicIfNotNil(err)
	db.binData = f
}

func (db *DB) closeBinData() error {
	return db.binData.Close()
}

func (db *DB) openBinLogCopy() *os.File {
	if !db.exists(db.option.Path) {
		panicIfNotNil(os.MkdirAll(db.option.Path, 0755))
	}
	f, err := os.OpenFile(binLogCopyPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	panicIfNotNil(err)
	return f
}

func (db *DB) openBinDataCopy() *os.File {
	if !db.exists(db.option.Path) {
		panicIfNotNil(os.MkdirAll(db.option.Path, 0755))
	}
	f, err := os.OpenFile(binDataCopyPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	panicIfNotNil(err)
	return f
}

func (db *DB) size() int64 {
	logInfo, err := db.binLog.Stat()
	panicIfNotNil(err)

	dataInfo, err := db.binData.Stat()
	panicIfNotNil(err)

	return logInfo.Size() + dataInfo.Size()
}

func (db *DB) compressed() {

	db.comMux.Lock()
	defer db.comMux.Unlock()

	var info, err = db.binLog.Stat()
	panicIfNotNil(err)
	if info.Size() == 0 {
		return
	}

	db.mux.Lock()

	_ = db.binLog.Close()
	_ = db.binData.Close()

	// rename
	panicIfNotNil(os.Rename(binDataPath, binDataCopyPath))
	panicIfNotNil(os.Rename(binLogPath, binLogCopyPath))

	db.openBinLog()
	db.openBinData()

	db.mux.Unlock()

	// rewrite

	var res []byte
	var counter = 0
	var size = 1024 * 1024 * 256

	for i := 0; i < len(db.data); i++ {
		for key, item := range db.data[i] {
			switch item.tp {
			case STRING:
				var d = encodeString(str2bytes(key), item)
				res = append(res, d...)
				counter += len(d)
			case LIST:
				var d = encodeList(str2bytes(key), item)
				res = append(res, d...)
				counter += len(d)
			case HASH:
				var d = encodeHash(str2bytes(key), item)
				res = append(res, d...)
				counter += len(d)
			}

			if counter >= size {
				panicIfNotNil(db.binData.Write(res[0:counter]))
				res = res[counter:]
				counter = 0
			}
		}
	}

	panicIfNotNil(db.binData.Write(res))

	panicIfNotNil(os.Remove(binLogCopyPath))
	panicIfNotNil(os.Remove(binDataCopyPath))

}

func (db *DB) read(bts []byte) uint64 {
	var reader = reader()
	return reader(bts, func(message []byte) {
		var command = Command(message[0])

		switch command {
		case HSET:
			key, k, v := decodeHSet(message)
			var sk = bytes2str(key)
			var dataMap = db.getDataMap(key)
			if dataMap[sk] == nil {
				dataMap[sk] = &base{
					// key: key,
					ttl: 0, tp: HASH,
					data: &Hash{
						data: map[string][]byte{bytes2str(k): v},
					},
				}
			} else {
				var hash = dataMap[sk].data.(*Hash)
				hash.data[bytes2str(k)] = v
			}
		case HDEL:
			key, k := decodeHDel(message)
			var sk = bytes2str(key)
			var dataMap = db.getDataMap(key)
			var hash = dataMap[sk].data.(*Hash)
			delete(hash.data, bytes2str(k))
			if len(hash.data) == 0 {
				delete(dataMap, sk)
			}
		case SET:
			key, v := decodeSet(message)
			var sk = bytes2str(key)
			var dataMap = db.getDataMap(key)
			if dataMap[sk] == nil {
				dataMap[sk] = &base{
					// key: key,
					ttl: 0, tp: STRING,
					data: &String{
						data: v,
					},
				}
			} else {
				var str = dataMap[sk].data.(*String)
				str.data = v
			}
		case DEL:
			key := decodeDel(message)
			var dataMap = db.getDataMap(key)
			delete(dataMap, bytes2str(key))
		case TTL:
			ttl, key := decodeTTL(message)
			var dataMap = db.getDataMap(key)
			if ttl != 0 && ttl < time.Now().UnixNano() {
				delete(dataMap, bytes2str(key))
			} else {
				dataMap[bytes2str(key)].ttl = ttl
			}
		case LPUSH:
			key, v := decodeLPush(message)
			var sk = bytes2str(key)
			var dataMap = db.getDataMap(key)
			if dataMap[sk] == nil {
				dataMap[sk] = &base{
					// key: key,
					ttl: 0, tp: LIST,
					data: &List{
						data: [][]byte{v},
					},
				}
			} else {
				var list = dataMap[sk].data.(*List)
				list.data = append([][]byte{v}, list.data...)
			}
		case RPOP:
			key := decodeRPop(message)
			var sk = bytes2str(key)
			var dataMap = db.getDataMap(key)
			var list = dataMap[sk].data.(*List)
			list.data = list.data[0 : len(list.data)-1]
			if len(list.data) == 0 {
				delete(dataMap, sk)
			}
		case LREM:
			index, key := decodeLRem(message)
			var sk = bytes2str(key)
			var dataMap = db.getDataMap(key)
			var list = dataMap[sk].data.(*List)
			l := len(list.data)
			if index < 0 {
				panic("index is less than 0")
			}
			if index > l-1 {
				panic("index overflow")
			}

			list.data = append(list.data[0:index], list.data[index+1:]...)
			if len(list.data) == 0 {
				delete(dataMap, sk)
			}
		case LSET:

			index, key, v := decodeLSet(message)
			var sk = bytes2str(key)
			var dataMap = db.getDataMap(key)
			var list = dataMap[sk].data.(*List)
			l := len(list.data)
			if index < 0 {
				panic("index is less than 0")
			}
			if index > l-1 {
				panic("index overflow")
			}

			list.data = append(list.data, nil)

			for i := l; i > index; i-- {
				list.data[l] = list.data[l-1]
			}

			list.data[index] = v
		default:
			panic("unknown command")
		}
	})
}

func (db *DB) load(r io.Reader, incIndex bool) []byte {

	if incIndex {
		var buf = make([]byte, 8)
		_, err := db.indexFile.ReadAt(buf, 0)
		if err != nil {
			db.index = 0
		} else {
			db.index = binary.LittleEndian.Uint64(buf)
		}
	}

	allBytes, err := ioutil.ReadAll(r)
	panicIfNotNil(err)

	var counter = db.read(allBytes)

	if incIndex {
		db.index += counter
	}

	if incIndex {
		var buf = make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, db.index)
		panicIfNotNil(db.indexFile.WriteAt(buf, 0))
	}

	return allBytes
}
