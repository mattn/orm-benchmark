package benchs

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

var sqlxdb *sqlx.DB

func init() {
	st := NewSuite("sqlx")
	st.InitF = func() {
		st.AddBenchmark("Insert", 2000*ORM_MULTI, SqlxInsert)
		st.AddBenchmark("MultiInsert 100 row", 500*ORM_MULTI, SqlxInsertMulti)
		st.AddBenchmark("Update", 2000*ORM_MULTI, SqlxUpdate)
		st.AddBenchmark("Read", 4000*ORM_MULTI, SqlxRead)
		st.AddBenchmark("MultiRead limit 100", 2000*ORM_MULTI, SqlxReadSlice)

		db, err := sqlx.Connect("postgres", ORM_SOURCE)
		checkErr(err)
		sqlxdb = db
	}
}

func SqlxInsert(b *B) {
	var m *Model
	wrapExecute(b, func() {
		initDB()
		m = NewModel()
	})
	for i := 0; i < b.N; i++ {
		sqlxdb.MustExec(`INSERT INTO models (name, title, fax, web, age, "right", counter) VALUES ($1, $2, $3, $4, $5, $6, $7)`, m.Name, m.Title, m.Fax, m.Web, m.Age, m.Right, m.Counter)
	}
}

func SqlxInsertMulti(b *B) {
	var ms []interface{}
	wrapExecute(b, func() {
		initDB()
		ms = make([]interface{}, 0, 100)
		for i := 0; i < 100; i++ {
			ms = append(ms, NewModel())
		}
	})

	for i := 0; i < b.N; i++ {
		if _, err := sqlxdb.NamedExec(`INSERT INTO models (name, title, fax, web, age, "right", counter) VALUES (:name, :title, :fax, :web, :age, :right, :counter)`, ms); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}

func SqlxUpdate(b *B) {
	var m *Model
	wrapExecute(b, func() {
		initDB()
		m = NewModel()
		sqlxdb.MustExec(`INSERT INTO models (name, title, fax, web, age, "right", counter) VALUES ($1, $2, $3, $4, $5, $6, $7)`, m.Name, m.Title, m.Fax, m.Web, m.Age, m.Right, m.Counter)
		m.Id = 1
	})
	for i := 0; i < b.N; i++ {
		if _, err := sqlxdb.NamedExec(`UPDATE models SET name = :name, title = :title, fax = :fax, web = :web, age = :age, "right" = :right, counter = :counter WHERE id = :id`, m); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}

func SqlxRead(b *B) {
	var m *Model
	wrapExecute(b, func() {
		initDB()
		m = NewModel()
		sqlxdb.MustExec(`INSERT INTO models (name, title, fax, web, age, "right", counter) VALUES ($1, $2, $3, $4, $5, $6, $7)`, m.Name, m.Title, m.Fax, m.Web, m.Age, m.Right, m.Counter)
	})
	for i := 0; i < b.N; i++ {
		m := []Model{}
		if err := sqlxdb.Select(&m, "SELECT * FROM models"); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}

func SqlxReadSlice(b *B) {
	panic(fmt.Errorf("in preparation"))
}
