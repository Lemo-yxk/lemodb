/**
* @program: lemodb
*
* @description:
*
* @author: lemo
*
* @create: 2020-06-21 20:55
**/

package lemodb

import (
	"fmt"
	"time"
)

func (db *DB) Set(key string, value string) error {
	db.mux.Lock()
	defer db.mux.Unlock()

	var dataMap = db.getDataMap([]byte(key))
	if dataMap[key] != nil && dataMap[key].tp != STRING {
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

	if dataMap[key] == nil {
		if db.isTranRunning {
			panicIfNotNil(db.binTran.Write(encodeSet(k, v)))
			return nil
		}

		dataMap[key] = &base{
			// key: k,
			ttl: 0, tp: STRING,
			data: &String{
				data: v,
			},
		}
	} else {
		var str = dataMap[key].data.(*String)
		if string(str.data) == value {
			return nil
		}

		if db.isTranRunning {
			panicIfNotNil(db.binTran.Write(encodeSet(k, v)))
			return nil
		}

		str.data = v
	}

	panicIfNotNil(db.binLog.Write(encodeSet(k, v)))
	db.index++

	return nil
}

func (db *DB) Get(key string) (*String, error) {
	db.mux.RLock()
	defer db.mux.RUnlock()

	var dataMap = db.getDataMap([]byte(key))
	var item = dataMap[key]

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
