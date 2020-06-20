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
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
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
	once      sync.Once
	comMux    sync.RWMutex

	data map[string]*base
}

var binDataPath = ""
var binLogPath = ""
var binDataCopyPath = ""
var binLogCopyPath = ""
var indexPath = ""

func (db *DB) Start() {
	db.once.Do(func() {

		if db.option == nil {
			panic("option is nil")
		}

		if db.option.Path == "" {
			panic("path is nil")
		}

		log = db.option.Logger

		var t = time.Now()

		var absPath, err = filepath.Abs(db.option.Path)
		panicIfNotNil(err)

		db.option.Path = absPath

		db.data = make(map[string]*base)

		binDataPath = path.Join(db.option.Path, "bindata")
		binLogPath = path.Join(db.option.Path, "binlog")
		indexPath = path.Join(db.option.Path, "index")
		binLogCopyPath = path.Join(db.option.Path, "binlogcopy")
		binDataCopyPath = path.Join(db.option.Path, "bindatacopy")

		db.openFile()

		db.load(db.binData, false)

		db.load(db.binLog, true)

		db.compressed()

		log.Infof("start success in %d ms\n", time.Now().Sub(t).Milliseconds())
	})
}

func (db *DB) Close() {
	db.mux.Lock()
	defer db.mux.Unlock()
	_ = db.indexFile.Close()
	_ = db.binLog.Close()
	_ = db.binData.Close()
	_ = db.indexFile.Sync()
	_ = db.binLog.Sync()
	_ = db.binData.Sync()
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

func (db *DB) compressed() {

	var info, err = db.binLog.Stat()
	panicIfNotNil(err)
	if info.Size() == 0 {
		return
	}

	db.comMux.Lock()

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
	for _, item := range db.data {

		switch item.tp {
		case STRING:
			var d = encodeString(item)
			res = append(res, d...)
			counter += len(d)
		case LIST:
			var d = encodeList(item)
			res = append(res, d...)
			counter += len(d)
		case HASH:
			var d = encodeHash(item)
			res = append(res, d...)
			counter += len(d)
		}

		if counter >= size {
			panicIfNotNil(db.binData.Write(res[0:counter]))
			res = res[counter:]
			counter = 0
		}
	}

	panicIfNotNil(db.binData.Write(res))

	panicIfNotNil(os.Remove(binLogCopyPath))
	panicIfNotNil(os.Remove(binDataCopyPath))

	db.comMux.Unlock()
}

func (db *DB) DropAll() {
	db.mux.Lock()
	defer db.mux.Unlock()
	db.data = make(map[string]*base)
	db.index = 0
	panicIfNotNil(db.binData.Truncate(0))
	panicIfNotNil(db.binLog.Truncate(0))
	panicIfNotNil(db.indexFile.Truncate(0))
}

func (db *DB) Index() uint64 {
	db.mux.RLock()
	defer db.mux.RUnlock()
	return db.index
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

	var reader = reader()

	allBytes, err := ioutil.ReadAll(r)
	panicIfNotNil(err)

	reader(allBytes, func(message []byte) {
		var command = Command(message[0])

		switch command {
		case HSET:
			key, k, v := decodeHSet(message)
			var sk = string(key)
			if db.data[sk] == nil {
				db.data[sk] = &base{
					key: key, ttl: 0, tp: HASH,
					data: &Hash{
						data: map[string]*Val{string(k): {meta: 0, value: v}},
					},
				}
			} else {
				var hash = db.data[sk].data.(*Hash)
				if hash.data[string(k)] == nil {
					hash.data[string(k)] = &Val{meta: 0, value: v}
				} else {
					hash.data[string(k)].value = v
				}
			}
		case HDEL:
			key, k := decodeHDel(message)
			var sk = string(key)
			var hash = db.data[sk].data.(*Hash)
			delete(hash.data, string(k))
			if len(hash.data) == 0 {
				delete(db.data, sk)
			}
		case SET:
			key, v := decodeSet(message)
			var k = string(key)
			if db.data[k] == nil {
				db.data[k] = &base{
					key: key, ttl: 0, tp: STRING,
					data: &String{
						data: &Val{meta: 0, value: v},
					},
				}
			} else {
				var str = db.data[k].data.(*String)
				str.data.value = v
			}
		case DEL:
			key := decodeDel(message)
			delete(db.data, string(key))
		case TTL:
			ttl, key := decodeTTL(message)
			if ttl != 0 && ttl < time.Now().UnixNano() {
				delete(db.data, string(key))
			} else {
				db.data[string(key)].ttl = ttl
			}
		case LPUSH:
			key, v := decodeLPush(message)
			var k = string(key)
			if db.data[k] == nil {
				db.data[k] = &base{
					key: key, ttl: 0, tp: LIST,
					data: &List{
						data: []*Val{{meta: 0, value: v}},
					},
				}
			} else {
				var list = db.data[k].data.(*List)
				list.data = append([]*Val{{meta: 0, value: v}}, list.data...)
			}
		case RPOP:
			key := decodeRPop(message)
			var k = string(key)
			var list = db.data[k].data.(*List)
			list.data = list.data[0 : len(list.data)-1]
			if len(list.data) == 0 {
				delete(db.data, k)
			}
		case LREM:
			index, key := decodeLRem(message)
			var k = string(key)
			var list = db.data[k].data.(*List)
			l := len(list.data)
			if index < 0 {
				panic("index is less than 0")
			}
			if index > l-1 {
				panic("index overflow")
			}

			list.data = append(list.data[0:index], list.data[index+1:]...)
			if len(list.data) == 0 {
				delete(db.data, k)
			}
		case LSET:
			// TODO
			// if not exists, return nil
			index, key, v := decodeLSet(message)
			var k = string(key)
			var list = db.data[k].data.(*List)
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

			list.data[index] = &Val{value: v, meta: 0}
		case META:
			meta, key := decodeMeta(message)
			db.data[string(key)].data.(*String).data.meta = meta
		case LMETA:
			meta, index, key := decodeLMeta(message)
			var k = string(key)
			var list = db.data[k].data.(*List)
			l := len(list.data)
			if index < 0 {
				panic("index is less than 0")
			}
			if index > l-1 {
				panic("index overflow")
			}

			list.data[index].meta = meta
		case HMETA:
			meta, key, k := decodeHMeta(message)
			var sk = string(key)
			var hash = db.data[sk].data.(*Hash)
			hash.data[string(k)].meta = meta
		default:
			panic("unknown command")
		}

		if incIndex {
			db.index++
		}
	})

	if incIndex {
		var buf = make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, db.index)
		panicIfNotNil(db.indexFile.WriteAt(buf, 0))
	}

	return allBytes
}

func (db *DB) Restore(r io.Reader) {
	db.mux.Lock()
	defer db.mux.Unlock()

	var buf = make([]byte, 8)
	_, err := r.Read(buf)
	if err != nil {
		db.index = 0
	} else {
		db.index = binary.LittleEndian.Uint64(buf)
	}

	db.data = make(map[string]*base)
	panicIfNotNil(db.binData.Truncate(0))
	panicIfNotNil(db.binLog.Truncate(0))
	panicIfNotNil(db.indexFile.WriteAt(buf, 0))
	panicIfNotNil(db.binData.Write(db.load(r, false)))
}

func (db *DB) Backup(w io.Writer) {
	db.mux.Lock()

	var buf = make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, db.index)

	var res []byte

	res = append(res, buf...)

	for _, item := range db.data {
		switch item.tp {
		case STRING:
			var d = encodeString(item)
			res = append(res, d...)
		case LIST:
			var d = encodeList(item)
			res = append(res, d...)
		case HASH:
			var d = encodeHash(item)
			res = append(res, d...)
		}
	}

	db.mux.Unlock()

	panicIfNotNil(w.Write(res))
}

func (db *DB) Count() int {
	db.mux.RLock()
	defer db.mux.RUnlock()
	return len(db.data)
}

func (db *DB) Get(key string) (*String, error) {
	db.mux.RLock()
	defer db.mux.RUnlock()
	var item = db.data[key]

	if item == nil {
		return nil, fmt.Errorf("%s: not found", key)
	}

	if item.tp != STRING {
		return nil, fmt.Errorf("%s: is not string type", key)
	}

	if item.ttl != 0 && item.ttl < time.Now().UnixNano() {
		return nil, fmt.Errorf("expired: %d ms", (item.ttl-time.Now().UnixNano())/1e6)
	}

	return item.data.(*String), nil
}

func (db *DB) HGet(key string, k string) (*Val, error) {
	db.mux.RLock()
	defer db.mux.RUnlock()

	if db.data[key] == nil {
		return nil, fmt.Errorf("%s: not found", key)
	}

	if db.data[key].tp != HASH {
		return nil, fmt.Errorf("%s: is not hash type", key)
	}

	if db.data[key].ttl != 0 && db.data[key].ttl < time.Now().UnixNano() {
		return nil, fmt.Errorf("expired: %d ms", (db.data[key].ttl-time.Now().UnixNano())/1e6)
	}

	var hash = db.data[key].data.(*Hash)

	if hash.data[k] == nil {
		return nil, fmt.Errorf("%s: not found in hash", k)
	}

	return hash.data[k], nil
}

func (db *DB) HGetAll(key string) (*Hash, error) {
	db.mux.RLock()
	defer db.mux.RUnlock()

	if db.data[key] == nil {
		return nil, fmt.Errorf("%s: not found", key)
	}

	if db.data[key].tp != HASH {
		return nil, fmt.Errorf("%s: is not hash type", key)
	}

	if db.data[key].ttl != 0 && db.data[key].ttl < time.Now().UnixNano() {
		return nil, fmt.Errorf("expired: %d ms", (db.data[key].ttl-time.Now().UnixNano())/1e6)
	}

	return db.data[key].data.(*Hash), nil
}

func (db *DB) Set(key string, value string) error {
	db.mux.Lock()
	defer db.mux.Unlock()

	if db.data[key] != nil && db.data[key].tp != STRING {
		return fmt.Errorf("%s: is not string type", key)
	}

	var k = []byte(key)
	var v = []byte(value)

	if err := checkKey(k); err != nil {
		return err
	}
	if err := checkValue(v); err != nil {
		return err
	}

	if db.data[key] == nil {
		db.data[key] = &base{
			key: k, ttl: 0, tp: STRING,
			data: &String{
				data: &Val{meta: 0, value: v},
			},
		}
	} else {
		db.data[key].data.(*String).data.value = v
	}

	panicIfNotNil(db.binLog.Write(encodeSet(k, v)))
	db.index++

	return nil
}

func (db *DB) HSet(key string, k string, v string) error {
	db.mux.Lock()
	defer db.mux.Unlock()

	if db.data[key] != nil && db.data[key].tp != HASH {
		return fmt.Errorf("%s: is not hash type", key)
	}

	var kk = []byte(key)
	var hk = []byte(k)
	var hv = []byte(v)

	if err := checkKey(kk); err != nil {
		return err
	}
	if err := checkKey(hk); err != nil {
		return err
	}
	if err := checkValue(hv); err != nil {
		return err
	}

	if db.data[key] == nil {
		db.data[key] = &base{
			key: kk, ttl: 0, tp: HASH,
			data: &Hash{
				data: map[string]*Val{k: {meta: 0, value: hv}},
			},
		}
	} else {
		var hash = db.data[key].data.(*Hash)
		if hash.data[k] == nil {
			hash.data[k] = &Val{meta: 0, value: hv}
		} else {
			hash.data[k].value = hv
		}
	}

	panicIfNotNil(db.binLog.Write(encodeHSet(kk, hk, hv)))
	db.index++

	return nil
}

func (db *DB) Meta(key string, meta byte) error {
	db.mux.Lock()
	defer db.mux.Unlock()

	var item = db.data[key]
	if item == nil {
		return fmt.Errorf("%s: not found", key)
	}

	if item.tp != STRING {
		return fmt.Errorf("%s: is not string type", key)
	}

	item.data.(*String).data.meta = meta

	panicIfNotNil(db.binLog.Write(encodeMeta(meta, []byte(key))))
	db.index++

	return nil
}

func (db *DB) TTL(key string) (time.Duration, error) {
	db.mux.Lock()
	defer db.mux.Unlock()

	var item = db.data[key]
	if item == nil {
		return 0, fmt.Errorf("%s: not found", key)
	}

	if item.ttl == 0 {
		return 0, nil
	}

	if item.ttl < time.Now().UnixNano() {
		return 0, fmt.Errorf("expired: %d ms", (item.ttl-time.Now().UnixNano())/1e6)
	}

	return time.Duration(item.ttl - time.Now().UnixNano()), nil
}

func (db *DB) Expired(key string, ttl time.Duration) error {
	db.mux.Lock()
	defer db.mux.Unlock()

	var item = db.data[key]
	if item == nil {
		return fmt.Errorf("%s: not found", key)
	}

	var t = time.Now().Add(ttl).UnixNano()
	item.ttl = t

	panicIfNotNil(db.binLog.Write(encodeTTL([]byte(key), t)))
	db.index++

	return nil
}

func (db *DB) Del(key string) error {
	db.mux.Lock()
	defer db.mux.Unlock()

	var item = db.data[key]
	if item == nil {
		return fmt.Errorf("%s: not found", key)
	}

	delete(db.data, key)

	panicIfNotNil(db.binLog.Write(encodeDel([]byte(key))))
	db.index++

	return nil
}

func (db *DB) HDel(key string, k string) error {
	db.mux.Lock()
	defer db.mux.Unlock()

	var item = db.data[key]
	if item == nil {
		return fmt.Errorf("%s: not found", key)
	}

	if item.tp != HASH {
		return fmt.Errorf("%s: is not hash type", key)
	}

	var hash = item.data.(*Hash)

	if hash.data[k] == nil {
		return fmt.Errorf("%s: not found in hash", k)
	}

	delete(hash.data, k)

	if len(hash.data) == 0 {
		delete(db.data, key)
	}

	panicIfNotNil(db.binLog.Write(encodeHDel([]byte(key), []byte(k))))
	db.index++

	return nil
}

func (db *DB) HMeta(meta byte, key string, k string) error {
	db.mux.Lock()
	defer db.mux.Unlock()

	var item = db.data[key]
	if item == nil {
		return fmt.Errorf("%s: not found", key)
	}

	if item.tp != HASH {
		return fmt.Errorf("%s: is not hash type", key)
	}

	var hash = item.data.(*Hash)

	if hash.data[k] == nil {
		return fmt.Errorf("%s: not found in hash", k)
	}

	hash.data[k].meta = meta

	panicIfNotNil(db.binLog.Write(encodeHMeta(meta, []byte(key), []byte(k))))
	db.index++

	return nil
}

func (db *DB) LMeta(meta byte, key string, index int) error {
	db.mux.Lock()
	defer db.mux.Unlock()

	var item = db.data[key]
	if item == nil {
		return fmt.Errorf("%s: not found", key)
	}

	if item.tp != LIST {
		return fmt.Errorf("%s: is not list type", key)
	}

	var list = item.data.(*List)

	var l = len(list.data)

	if index < 0 {
		return fmt.Errorf("%d: less than 0", index)
	}
	if index > l-1 {
		return fmt.Errorf("%d: overflow %d", index, l-1)
	}

	list.data[index].meta = meta

	panicIfNotNil(db.binLog.Write(encodeLMeta(meta, index, []byte(key))))
	db.index++

	return nil
}

func (db *DB) LPush(key string, value ...string) error {
	db.mux.Lock()
	defer db.mux.Unlock()

	var l = len(value)
	if l == 0 {
		return fmt.Errorf("value is empty")
	}

	if db.data[key] != nil && db.data[key].tp != LIST {
		return fmt.Errorf("%s: is not list type", key)
	}

	var k = []byte(key)
	if err := checkKey(k); err != nil {
		return err
	}
	for i := 0; i < len(value); i++ {
		if err := checkValue([]byte(value[i])); err != nil {
			return err
		}
	}

	if db.data[key] == nil {
		db.data[key] = &base{
			key: k, ttl: 0, tp: LIST,
			data: &List{
				data: []*Val{},
			},
		}
	}

	var list = db.data[key].data.(*List)

	for i := 0; i < len(value); i++ {
		list.data = append([]*Val{{meta: 0, value: []byte(value[i])}}, list.data...)
		panicIfNotNil(db.binLog.Write(encodeLPush(k, []byte(value[i]))))
		db.index++
	}

	return nil
}

func (db *DB) RPop(key string) (*Val, error) {
	db.mux.Lock()
	defer db.mux.Unlock()

	var item = db.data[key]

	if item == nil {
		return nil, fmt.Errorf("%s: not found", key)
	}

	if item.tp != LIST {
		return nil, fmt.Errorf("%s: is not list type", key)
	}

	var list = item.data.(*List)

	var l = len(list.data)

	var value = list.data[l-1]

	list.data = list.data[0 : l-1]

	if len(list.data) == 0 {
		delete(db.data, key)
	}

	if item.ttl != 0 && item.ttl < time.Now().UnixNano() {
		return nil, fmt.Errorf("expired: %d ms", (item.ttl-time.Now().UnixNano())/1e6)
	}

	panicIfNotNil(db.binLog.Write(encodeRPop([]byte(key))))
	db.index++

	return value, nil
}

func (db *DB) LRemV(key string, value string) (*Val, error) {
	db.mux.Lock()
	defer db.mux.Unlock()

	var item = db.data[key]

	if item == nil {
		return nil, fmt.Errorf("%s: not found", key)
	}

	if item.tp != LIST {
		return nil, fmt.Errorf("%s: is not list type", key)
	}

	var list = item.data.(*List)

	var index = -1
	for i := 0; i < len(list.data); i++ {
		if string(list.data[i].value) == value {
			index = i
			break
		}
	}

	if index == -1 {
		return nil, fmt.Errorf("%s: %s not found", key, value)
	}

	var remVal = list.data[index]

	list.data = append(list.data[0:index], list.data[index+1:]...)

	if len(list.data) == 0 {
		delete(db.data, key)
	}

	if item.ttl != 0 && item.ttl < time.Now().UnixNano() {
		return nil, fmt.Errorf("expired: %d ms", (item.ttl-time.Now().UnixNano())/1e6)
	}

	panicIfNotNil(db.binLog.Write(encodeLRem([]byte(key), index)))
	db.index++

	return remVal, nil
}

func (db *DB) LRem(key string, index int) (*Val, error) {
	db.mux.Lock()
	defer db.mux.Unlock()

	var item = db.data[key]

	if item == nil {
		return nil, fmt.Errorf("%s: not found", key)
	}

	if db.data[key] != nil && db.data[key].tp != LIST {
		return nil, fmt.Errorf("%s: is not list type", key)
	}

	var list = item.data.(*List)

	var l = len(list.data)

	if index < 0 {
		return nil, fmt.Errorf("%d: less than 0", index)
	}
	if index > l-1 {
		return nil, fmt.Errorf("%d: overflow %d", index, l-1)
	}

	var remVal = list.data[index]

	list.data = append(list.data[0:index], list.data[index+1:]...)

	if len(list.data) == 0 {
		delete(db.data, key)
	}

	if item.ttl != 0 && item.ttl < time.Now().UnixNano() {
		return nil, fmt.Errorf("expired: %d ms", (item.ttl-time.Now().UnixNano())/1e6)
	}

	panicIfNotNil(db.binLog.Write(encodeLRem([]byte(key), index)))
	db.index++

	return remVal, nil
}

func (db *DB) LSet(key string, index int, value string) error {
	db.mux.Lock()
	defer db.mux.Unlock()

	var item = db.data[key]

	if item == nil {
		return fmt.Errorf("%s: not found", key)
	}

	if item.tp != LIST {
		return fmt.Errorf("%s: is not list type", key)
	}

	var k = []byte(key)
	var v = []byte(value)
	if err := checkKey(k); err != nil {
		return err
	}
	if err := checkValue(v); err != nil {
		return err
	}

	var list = item.data.(*List)

	var l = len(list.data)

	if index < 0 {
		return fmt.Errorf("%d: less than 0", index)
	}
	if index > l-1 {
		return fmt.Errorf("%d: overflow %d", index, l-1)
	}

	list.data = append(list.data, nil)

	for i := l; i > index; i-- {
		list.data[l] = list.data[l-1]
	}

	list.data[index] = &Val{value: v, meta: 0}

	panicIfNotNil(db.binLog.Write(encodeLSet(k, index, v)))
	db.index++

	return nil
}

func (db *DB) List(key string) (*List, error) {
	db.mux.Lock()
	defer db.mux.Unlock()

	var item = db.data[key]
	if item == nil {
		return nil, fmt.Errorf("%s: not found", key)
	}

	if item.tp != LIST {
		return nil, fmt.Errorf("%s: is not list type", key)
	}

	if item.ttl != 0 && item.ttl < time.Now().UnixNano() {
		return nil, fmt.Errorf("expired: %d ms", (item.ttl-time.Now().UnixNano())/1e6)
	}

	return item.data.(*List), nil
}

func (db *DB) Keys(fn func(tp Type, ttl int64, key string) bool) {
	for key, value := range db.data {
		if !fn(value.tp, time.Duration(value.ttl).Milliseconds(), key) {
			return
		}
	}
}
