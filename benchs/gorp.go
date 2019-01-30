package benchs

import (
	"fmt"

	"database/sql"

	_ "github.com/lib/pq"
	"gopkg.in/gorp.v1"
)

var dbmap *gorp.DbMap

func init() {
	st := NewSuite("gorp")
	st.InitF = func() {
		st.AddBenchmark("Insert", 2000*ORM_MULTI, GorpInsert)
		st.AddBenchmark("MultiInsert 100 row", 500*ORM_MULTI, GorpInsertMulti)
		st.AddBenchmark("Update", 2000*ORM_MULTI, GorpUpdate)
		st.AddBenchmark("Read", 4000*ORM_MULTI, GorpRead)
		st.AddBenchmark("MultiRead limit 100", 2000*ORM_MULTI, GorpReadSlice)

		db, err := sql.Open("postgres", ORM_SOURCE)
		checkErr(err)
		dbmap = &gorp.DbMap{Db: db, Dialect: gorp.PostgresDialect{}}
		dbmap.AddTableWithName(Model{}, "models").SetKeys(true, "id")
	}
}

func GorpInsert(b *B) {
	var m *Model
	wrapExecute(b, func() {
		initDB()
		m = NewModel()
	})
	for i := 0; i < b.N; i++ {
		m.Id = 0
		err := dbmap.Insert(m)
		if err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}

func GorpInsertMulti(b *B) {
	var ms []interface{}
	wrapExecute(b, func() {
		initDB()
		ms = make([]interface{}, 0, 100)
		for i := 0; i < 100; i++ {
			ms = append(ms, NewModel())
		}
	})

	for i := 0; i < b.N; i++ {
		tx, err := dbmap.Begin()
		if err != nil {
			fmt.Println(err)
			b.FailNow()
		}
		if err := tx.Insert(ms...); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
		tx.Commit()
	}
}

func GorpUpdate(b *B) {
	var m *Model
	wrapExecute(b, func() {
		initDB()
		m = NewModel()
	})
	for i := 0; i < b.N; i++ {
		m.Id = 0
		_, err := dbmap.Update(m)
		if err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}

func GorpRead(b *B) {
	var m *Model
	wrapExecute(b, func() {
		initDB()
		m = NewModel()
		err := dbmap.Insert(m)
		if err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	})
	for i := 0; i < b.N; i++ {
		err := dbmap.SelectOne(m, "SELECT * FROM models WHERE id = $1", 1)
		if err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}

func GorpReadSlice(b *B) {
	var m *Model
	wrapExecute(b, func() {
		initDB()
		m = NewModel()
		for i := 0; i < 100; i++ {
			m.Id = 0
			err := dbmap.Insert(m)
			if err != nil {
				fmt.Println(err)
				b.FailNow()
			}
		}
	})

	for i := 0; i < b.N; i++ {
		var models []*Model
		_, err := dbmap.Select(&models, "SELECT * FROM models WHERE id > $1 LIMIT 100", 0)
		if err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}
