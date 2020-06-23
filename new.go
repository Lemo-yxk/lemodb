/**
* @program: lemodb
*
* @description:
*
* @author: lemo
*
* @create: 2020-06-13 14:27
**/

package lemodb

func Open(option *Option) *DB {

	var db = &DB{}

	db.option = option

	db.Start()

	return db
}
