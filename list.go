/**
* @program: lemodb
*
* @description:
*
* @author: lemo
*
* @create: 2020-06-21 20:58
**/

package lemodb

import (
	"fmt"
	"time"
)

func (db *DB) LPush(key string, value ...string) error {
	db.mux.Lock()
	defer db.mux.Unlock()

	var l = len(value)
	if l == 0 {
		return fmt.Errorf("value is empty")
	}

	var dataMap = db.getDataMap([]byte(key))
	if dataMap[key] != nil && dataMap[key].tp != LIST {
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

	if db.isTranRunning {
		for i := 0; i < len(value); i++ {
			panicIfNotNil(db.binTran.Write(encodeLPush(k, []byte(value[i]))))
		}
		return nil
	}

	if dataMap[key] == nil {
		dataMap[key] = &base{
			// key: k,
			ttl: 0, tp: LIST,
			data: &List{
				data: [][]byte{},
			},
		}
	}

	var list = dataMap[key].data.(*List)

	for i := 0; i < len(value); i++ {
		list.data = append([][]byte{[]byte(value[i])}, list.data...)
		panicIfNotNil(db.binLog.Write(encodeLPush(k, []byte(value[i]))))
		db.index++
	}

	return nil
}

func (db *DB) RPop(key string) (string, error) {
	db.mux.Lock()
	defer db.mux.Unlock()

	var dataMap = db.getDataMap([]byte(key))
	var item = dataMap[key]

	if item == nil {
		return "", fmt.Errorf("%s: not found", key)
	}

	if item.tp != LIST {
		return "", fmt.Errorf("%s: is not list type", key)
	}

	if db.isTranRunning {
		panicIfNotNil(db.binTran.Write(encodeRPop([]byte(key))))
		return "", nil
	}

	var list = item.data.(*List)

	var l = len(list.data)

	var value = list.data[l-1]

	list.data = list.data[0 : l-1]

	if len(list.data) == 0 {
		delete(dataMap, key)
	}

	if item.ttl != 0 && item.ttl < time.Now().UnixNano() {
		return "", fmt.Errorf("expired: %d ms", (item.ttl-time.Now().UnixNano())/1e6)
	}

	panicIfNotNil(db.binLog.Write(encodeRPop([]byte(key))))
	db.index++

	return string(value), nil
}

func (db *DB) LRemV(key string, value string) (string, error) {
	db.mux.Lock()
	defer db.mux.Unlock()

	var dataMap = db.getDataMap([]byte(key))
	var item = dataMap[key]

	if item == nil {
		return "", fmt.Errorf("%s: not found", key)
	}

	if item.tp != LIST {
		return "", fmt.Errorf("%s: is not list type", key)
	}

	var list = item.data.(*List)

	var index = -1
	for i := 0; i < len(list.data); i++ {
		if string(list.data[i]) == value {
			index = i
			break
		}
	}

	if index == -1 {
		return "", fmt.Errorf("%s: %s not found", key, value)
	}

	if db.isTranRunning {
		panicIfNotNil(db.binTran.Write(encodeLRem([]byte(key), index)))
		return "", nil
	}

	var remVal = list.data[index]

	list.data = append(list.data[0:index], list.data[index+1:]...)

	if len(list.data) == 0 {
		delete(dataMap, key)
	}

	if item.ttl != 0 && item.ttl < time.Now().UnixNano() {
		return "", fmt.Errorf("expired: %d ms", (item.ttl-time.Now().UnixNano())/1e6)
	}

	panicIfNotNil(db.binLog.Write(encodeLRem([]byte(key), index)))
	db.index++

	return string(remVal), nil
}

func (db *DB) LRem(key string, index int) (string, error) {
	db.mux.Lock()
	defer db.mux.Unlock()

	var dataMap = db.getDataMap([]byte(key))
	var item = dataMap[key]

	if item == nil {
		return "", fmt.Errorf("%s: not found", key)
	}

	if dataMap[key] != nil && dataMap[key].tp != LIST {
		return "", fmt.Errorf("%s: is not list type", key)
	}

	var list = item.data.(*List)

	var l = len(list.data)

	if index < 0 {
		return "", fmt.Errorf("%d: less than 0", index)
	}
	if index > l-1 {
		return "", fmt.Errorf("%d: overflow %d", index, l-1)
	}

	if db.isTranRunning {
		panicIfNotNil(db.binTran.Write(encodeLRem([]byte(key), index)))
		return "", nil
	}

	var remVal = list.data[index]

	list.data = append(list.data[0:index], list.data[index+1:]...)

	if len(list.data) == 0 {
		delete(dataMap, key)
	}

	if item.ttl != 0 && item.ttl < time.Now().UnixNano() {
		return "", fmt.Errorf("expired: %d ms", (item.ttl-time.Now().UnixNano())/1e6)
	}

	panicIfNotNil(db.binLog.Write(encodeLRem([]byte(key), index)))
	db.index++

	return string(remVal), nil
}

func (db *DB) LSet(key string, index int, value string) error {
	db.mux.Lock()
	defer db.mux.Unlock()

	var dataMap = db.getDataMap([]byte(key))
	var item = dataMap[key]

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

	if db.isTranRunning {
		panicIfNotNil(db.binTran.Write(encodeLSet(k, index, v)))
		return nil
	}

	list.data = append(list.data, nil)

	for i := l; i > index; i-- {
		list.data[l] = list.data[l-1]
	}

	list.data[index] = v

	panicIfNotNil(db.binLog.Write(encodeLSet(k, index, v)))
	db.index++

	return nil
}

func (db *DB) List(key string) (*List, error) {
	db.mux.RLock()
	defer db.mux.RUnlock()

	var dataMap = db.getDataMap([]byte(key))
	var item = dataMap[key]
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
