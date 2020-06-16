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

func Open(path string) *DB {
	var option = &Option{
		Path:   path,
		Logger: new(defaultLogger),
	}

	var db = &DB{}

	db.option = option

	db.Start()

	return db
}
