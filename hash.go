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

	var dataMap = db.getDataMap([]byte(key))
	if dataMap[key] == nil {
		return "", fmt.Errorf("%s: not found", key)
	}

	if dataMap[key].tp != HASH {
		return "", fmt.Errorf("%s: is not hash type", key)
	}

	if dataMap[key].ttl != 0 && dataMap[key].ttl < time.Now().UnixNano() {
		return "", fmt.Errorf("expired: %d ms", (dataMap[key].ttl-time.Now().UnixNano())/1e6)
	}

	var hash = dataMap[key].data.(*Hash)

	if hash.data[k] == nil {
		return "", fmt.Errorf("%s: not found in hash", k)
	}

	return string(hash.data[k]), nil
}

func (db *DB) HGetAll(key string) (*Hash, error) {
	db.mux.RLock()
	defer db.mux.RUnlock()

	var dataMap = db.getDataMap([]byte(key))
	if dataMap[key] == nil {
		return nil, fmt.Errorf("%s: not found", key)
	}

	if dataMap[key].tp != HASH {
		return nil, fmt.Errorf("%s: is not hash type", key)
	}

	if dataMap[key].ttl != 0 && dataMap[key].ttl < time.Now().UnixNano() {
		return nil, fmt.Errorf("expired: %d ms", (dataMap[key].ttl-time.Now().UnixNano())/1e6)
	}

	return dataMap[key].data.(*Hash), nil
}

func (db *DB) HSet(key string, k string, v string) error {
	db.mux.Lock()
	defer db.mux.Unlock()

	var dataMap = db.getDataMap([]byte(key))
	if dataMap[key] != nil && dataMap[key].tp != HASH {
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

	if dataMap[key] == nil {
		if db.isTranRunning {
			panicIfNotNil(db.binTran.Write(encodeHSet(kk, hk, hv)))
			return nil
		}

		dataMap[key] = &base{
			// key: kk,
			ttl: 0, tp: HASH,
			data: &Hash{
				data: map[string][]byte{k: hv},
			},
		}

	} else {
		var hash = dataMap[key].data.(*Hash)

		if string(hash.data[k]) == v {
			return nil
		}

		if db.isTranRunning {
			panicIfNotNil(db.binTran.Write(encodeHSet(kk, hk, hv)))
			return nil
		}

		hash.data[k] = hv
	}

	panicIfNotNil(db.binLog.Write(encodeHSet(kk, hk, hv)))
	db.index++

	return nil
}

func (db *DB) HDel(key string, k string) error {
	db.mux.Lock()
	defer db.mux.Unlock()

	var dataMap = db.getDataMap([]byte(key))
	var item = dataMap[key]
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
		panicIfNotNil(db.binTran.Write(encodeHDel([]byte(key), []byte(k))))
		return nil
	}

	delete(hash.data, k)

	if len(hash.data) == 0 {
		delete(dataMap, key)
	}

	panicIfNotNil(db.binLog.Write(encodeHDel([]byte(key), []byte(k))))
	db.index++

	return nil
}
