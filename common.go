/**
* @program: lemodb
*
* @description:
*
* @author: lemo
*
* @create: 2020-06-21 20:56
**/

package lemodb

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"time"
)

func (db *DB) DropAll() {
	db.mux.Lock()
	defer db.mux.Unlock()
	db.newDataMap()
	db.index = 0
	panicIfNotNil(db.binData.Truncate(0))
	panicIfNotNil(db.binLog.Truncate(0))
	panicIfNotNil(db.indexFile.Truncate(0))
	db.isTranRunning = false
	db.binTran.Reset()
}

func (db *DB) Index() uint64 {
	db.mux.RLock()
	defer db.mux.RUnlock()
	return db.index
}

func (db *DB) Restore(r io.Reader) uint64 {
	db.mux.Lock()
	defer db.mux.Unlock()

	var buf = make([]byte, 8)
	_, err := r.Read(buf)
	if err != nil {
		db.index = 0
	} else {
		db.index = binary.LittleEndian.Uint64(buf)
	}

	db.newDataMap()
	panicIfNotNil(db.binData.Truncate(0))
	panicIfNotNil(db.binLog.Truncate(0))
	panicIfNotNil(db.indexFile.WriteAt(buf, 0))
	panicIfNotNil(db.binData.Write(db.load(r, false)))
	db.isTranRunning = false
	db.binTran.Reset()

	return db.index
}

func (db *DB) Backup(w io.Writer) uint64 {
	db.mux.Lock()
	defer db.mux.Unlock()

	var buf = make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, db.index)

	var res []byte

	res = append(res, buf...)

	for i := 0; i < len(db.data); i++ {
		for key, item := range db.data[i] {
			switch item.tp {
			case STRING:
				var d = encodeString([]byte(key), item)
				res = append(res, d...)
			case LIST:
				var d = encodeList([]byte(key), item)
				res = append(res, d...)
			case HASH:
				var d = encodeHash([]byte(key), item)
				res = append(res, d...)
			}
		}
	}

	panicIfNotNil(w.Write(res))

	return db.index
}

func (db *DB) Count() int {
	db.mux.RLock()
	defer db.mux.RUnlock()
	var n = 0
	for i := 0; i < len(db.data); i++ {
		n += len(db.data[i])
	}
	return n
}

func (db *DB) TTL(key string) (time.Duration, error) {
	db.mux.RLock()
	defer db.mux.RUnlock()

	var dataMap = db.getDataMap([]byte(key))
	var item = dataMap[key]
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

	var dataMap = db.getDataMap([]byte(key))
	var item = dataMap[key]
	if item == nil {
		return fmt.Errorf("%s: not found", key)
	}

	var t = time.Now().Add(ttl).UnixNano()

	if db.isTranRunning {
		panicIfNotNil(db.binTran.Write(encodeTTL([]byte(key), t)))
		return nil
	}

	item.ttl = t

	panicIfNotNil(db.binLog.Write(encodeTTL([]byte(key), t)))
	db.index++

	return nil
}

func (db *DB) Del(key string) error {
	db.mux.Lock()
	defer db.mux.Unlock()

	var dataMap = db.getDataMap([]byte(key))
	var item = dataMap[key]
	if item == nil {
		return fmt.Errorf("%s: not found", key)
	}

	if db.isTranRunning {
		panicIfNotNil(db.binTran.Write(encodeDel([]byte(key))))
		return nil
	}

	delete(dataMap, key)

	panicIfNotNil(db.binLog.Write(encodeDel([]byte(key))))
	db.index++

	return nil
}

func (db *DB) Keys(fn func(tp Type, ttl int64, key string) bool) {
	for i := 0; i < len(db.data); i++ {
		for key, value := range db.data[i] {
			if !fn(value.tp, time.Duration(value.ttl).Milliseconds(), key) {
				return
			}
		}
	}
}

func (db *DB) DelayStart() {
	db.mux.Lock()
	defer db.mux.Unlock()
	db.isTranRunning = true
	db.binTran.Reset()
}

func (db *DB) CleanDelayCommit() {
	db.mux.Lock()
	defer db.mux.Unlock()
	db.binTran.Reset()
}

func (db *DB) DelayCommit() uint64 {
	db.mux.Lock()
	defer db.mux.Unlock()
	if !db.isTranRunning {
		return db.index
	}

	var bts, err = ioutil.ReadAll(db.binTran)
	panicIfNotNil(err)

	if len(bts) == 0 {
		return db.index
	}

	var counter = db.read(bts)

	db.index += counter

	panicIfNotNil(db.binLog.Write(bts))

	return db.index
}

func (db *DB) DelayEnd() {
	db.mux.Lock()
	defer db.mux.Unlock()
	db.isTranRunning = false
}

func (db *DB) Transaction() {
	db.tranMux.Lock()
	db.DelayStart()
}

func (db *DB) Commit() uint64 {
	var index = db.DelayCommit()
	db.DelayEnd()
	db.tranMux.Unlock()
	return index
}
