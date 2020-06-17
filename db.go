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

	string map[string][]byte
	list   map[string][][]byte
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

		db.string = make(map[string][]byte)
		db.list = make(map[string][][]byte)

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
	for _, item := range db.string {
		res = append(res, item...)
	}

	for _, item := range db.list {
		for i := len(item) - 1; i >= 0; i-- {
			res = append(res, item[i]...)
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
	db.string = make(map[string][]byte)
	db.list = make(map[string][][]byte)
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

	reader(allBytes, func(bytes []byte) {
		ttl := getTTL(bytes)
		key := string(getKey(bytes))
		keyType := Type(getKeyType(bytes))

		switch keyType {
		case STRING:
			if ttl > 8 && time.Now().Unix() > int64(ttl) {
				delete(db.string, key)
				if incIndex {
					db.index++
				}
				return
			}
			db.string[key] = bytes
		case LIST:
			if ttl > 8 && time.Now().Unix() > int64(ttl) {
				delete(db.list, key)
				if incIndex {
					db.index++
				}
				return
			}
			if ttl == 1 {
				db.list[key] = db.list[key][0 : len(db.list[key])-1]
			} else {
				db.list[key] = append([][]byte{bytes}, db.list[key]...)
			}
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

	db.string = make(map[string][]byte)
	db.list = make(map[string][][]byte)
	panicIfNotNil(db.binData.Truncate(0))
	panicIfNotNil(db.binLog.Truncate(0))
	panicIfNotNil(db.indexFile.WriteAt(buf, 0))
	panicIfNotNil(db.binData.Write(db.load(r, false)))
}

func (db *DB) Backup(w io.Writer) {
	db.mux.Lock()

	var buf = make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(db.index))

	var res []byte

	res = append(res, buf...)

	for _, item := range db.string {
		res = append(res, item...)
	}

	for _, item := range db.list {
		for i := len(item) - 1; i >= 0; i-- {
			res = append(res, item[i]...)
		}
	}

	db.mux.Unlock()

	panicIfNotNil(w.Write(res))
}

func (db *DB) Count() int {
	db.mux.RLock()
	defer db.mux.RUnlock()
	return len(db.string) + len(db.list)
}

func (db *DB) Get(key string) *Item {
	db.mux.RLock()
	defer db.mux.RUnlock()
	var item = db.string[key]
	if item == nil {
		return nil
	}
	var ttl = getTTL(item)
	if ttl > 8 && time.Now().Unix() > int64(ttl) {
		return nil
	}
	keyType, meta, t, k, v := decode(item)
	return &Item{key: k, value: v, keyType: Type(keyType), meta: meta, ttl: t}
}

func (db *DB) SetEntry(entry *entry) {
	db.mux.Lock()
	defer db.mux.Unlock()

	if entry.ttl > 0 {
		entry.ttl += uint32(time.Now().Unix())
	}

	var item = encode(db.string[entry.key], byte(STRING), entry.meta, entry.ttl, []byte(entry.key), entry.value)

	db.string[entry.key] = item

	panicIfNotNil(db.binLog.Write(item))
	db.index++
}

func (db *DB) Set(key string, value []byte) {
	db.mux.Lock()
	defer db.mux.Unlock()

	var item = encode(db.string[key], byte(STRING), 0, 0, []byte(key), value)

	db.string[key] = item

	panicIfNotNil(db.binLog.Write(item))
	db.index++
}

type entry struct {
	key     string
	value   []byte
	meta    byte
	ttl     uint32
	keyType Type
}

func (entry *entry) WithTTL(ttl int) *entry {
	entry.ttl = uint32(ttl)
	return entry
}

func (entry *entry) WithMeta(meta byte) *entry {
	entry.meta = meta
	return entry
}

func NewEntry(key string, value []byte) *entry {
	return &entry{key: key, value: value}
}

func (db *DB) Del(key string) {
	db.mux.Lock()
	defer db.mux.Unlock()

	var item []byte

	if db.string[key] != nil {
		delete(db.string, key)
		item = encode(nil, byte(STRING), 0, 9, []byte(key), nil)
	}

	if db.list[key] != nil {
		delete(db.list, key)
		item = encode(nil, byte(LIST), 0, 9, []byte(key), nil)
	}

	panicIfNotNil(db.binLog.Write(item))
	db.index++
}

func (db *DB) LPush(key string, value ...[]byte) {
	db.mux.Lock()
	defer db.mux.Unlock()

	for i := 0; i < len(value); i++ {
		var item = encode(nil, byte(LIST), 0, 0, []byte(key), value[i])
		db.list[key] = append([][]byte{item}, db.list[key]...)
		panicIfNotNil(db.binLog.Write(item))
		db.index++
	}
}

func (db *DB) LPushEntry(entry *entry) {
	db.mux.Lock()
	defer db.mux.Unlock()
	if entry.ttl > 0 {
		entry.ttl += uint32(time.Now().Unix())
	}
	var item = encode(db.string[entry.key], byte(LIST), entry.meta, entry.ttl, []byte(entry.key), entry.value)
	db.list[entry.key] = append([][]byte{item}, db.list[entry.key]...)
	panicIfNotNil(db.binLog.Write(item))
	db.index++
}

func (db *DB) RPop(key string) *Item {
	db.mux.Lock()
	defer db.mux.Unlock()

	if len(db.list[key]) == 0 {
		return nil
	}

	var item = db.list[key][len(db.list[key])-1]
	db.list[key] = db.list[key][0 : len(db.list[key])-1]

	var ttl = getTTL(item)
	if ttl > 8 && time.Now().Unix() > int64(ttl) {
		return nil
	}

	panicIfNotNil(db.binLog.Write(encode(nil, byte(LIST), 0, 1, []byte(key), nil)))
	db.index++

	keyType, meta, t, k, v := decode(item)
	return &Item{key: k, value: v, keyType: Type(keyType), meta: meta, ttl: t}
}

func (db *DB) List(key string) []*Item {
	db.mux.Lock()
	defer db.mux.Unlock()

	var item = db.list[key]
	if item == nil {
		return nil
	}

	var res []*Item

	for i := 0; i < len(item); i++ {
		keyType, meta, t, k, v := decode(item[i])
		res = append(res, &Item{key: k, value: v, keyType: Type(keyType), meta: meta, ttl: t})
	}

	return res
}

func (db *DB) Range(fn func(keyType Type, meta byte, key string) bool) {
	for key, value := range db.string {
		if !fn(Type(value[0]), value[1], key) {
			return
		}
	}
	for key, values := range db.list {
		for i := 0; i < len(values); i++ {
			if !fn(Type(values[i][0]), values[i][1], key) {
				return
			}
		}
	}
}

func (db *DB) Values(fn func(item *Item) bool) {
	for _, value := range db.string {
		keyType, meta, t, k, v := decode(value)
		if !fn(&Item{key: k, value: v, keyType: Type(keyType), meta: meta, ttl: t}) {
			return
		}
	}
	for _, values := range db.list {
		for i := 0; i < len(values); i++ {
			keyType, meta, t, k, v := decode(values[i])
			if !fn(&Item{key: k, value: v, keyType: Type(keyType), meta: meta, ttl: t}) {
				return
			}
		}
	}
}

func (db *DB) Keys(fn func(key string) bool) {
	for key := range db.string {
		if !fn(key) {
			return
		}
	}
	for key := range db.list {
		if !fn(key) {
			return
		}
	}
}
