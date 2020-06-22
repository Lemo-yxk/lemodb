/**
* @program: lemodb
*
* @description:
*
* @author: lemo
*
* @create: 2020-06-21 20:59
**/

package lemodb

import (
	"fmt"
	"time"
)

func (db *DB) HGet(key string, k string) (string, error) {
	db.mux.RLock()
	defer db.mux.RUnlock()

	if db.data[key] == nil {
		return "", fmt.Errorf("%s: not found", key)
	}

	if db.data[key].tp != HASH {
		return "", fmt.Errorf("%s: is not hash type", key)
	}

	if db.data[key].ttl != 0 && db.data[key].ttl < time.Now().UnixNano() {
		return "", fmt.Errorf("expired: %d ms", (db.data[key].ttl-time.Now().UnixNano())/1e6)
	}

	var hash = db.data[key].data.(*Hash)

	if hash.data[k] == nil {
		return "", fmt.Errorf("%s: not found in hash", k)
	}

	return string(hash.data[k]), nil
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

	if db.isTranRunning {
		panicIfNotNil(db.writer.Write(encodeHSet(kk, hk, hv)))
		return nil
	}

	if db.data[key] == nil {
		db.data[key] = &base{
			key: kk, ttl: 0, tp: HASH,
			data: &Hash{
				data: map[string][]byte{k: hv},
			},
		}
	} else {
		var hash = db.data[key].data.(*Hash)
		hash.data[k] = hv
	}

	panicIfNotNil(db.writer.Write(encodeHSet(kk, hk, hv)))
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

	if db.isTranRunning {
		panicIfNotNil(db.writer.Write(encodeHDel([]byte(key), []byte(k))))
		return nil
	}

	delete(hash.data, k)

	if len(hash.data) == 0 {
		delete(db.data, key)
	}

	panicIfNotNil(db.writer.Write(encodeHDel([]byte(key), []byte(k))))
	db.index++

	return nil
}
