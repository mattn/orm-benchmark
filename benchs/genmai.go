package benchs

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/naoina/genmai"
)

var genmaidb *genmai.DB

func initDB_genmai() {

	sqls := []string{
		`DROP TABLE IF EXISTS genmai_model;`,
		`CREATE TABLE genmai_model (
			id SERIAL NOT NULL,
			name text NOT NULL,
			title text NOT NULL,
			fax text NOT NULL,
			web text NOT NULL,
			age integer NOT NULL,
			"right" boolean NOT NULL,
			counter bigint NOT NULL,
			CONSTRAINT genmai_model_pkey PRIMARY KEY (id)
			) WITH (OIDS=FALSE);`,
	}

	DB, err := sql.Open("postgres", ORM_SOURCE)
	checkErr(err)
	defer DB.Close()

	err = DB.Ping()
	checkErr(err)

	for _, sql := range sqls {
		_, err = DB.Exec(sql)
		checkErr(err)
	}
}

type GenmaiModel struct {
	Id      int `db:"pk"`
	Name    string
	Title   string
	Fax     string
	Web     string
	Age     int
	Right   bool
	Counter int64
}

func NewGenmaiModel() *GenmaiModel {
	m := new(GenmaiModel)
	m.Name = "Orm Benchmark"
	m.Title = "Just a Benchmark for fun"
	m.Fax = "99909990"
	m.Web = "http://blog.milkpod29.me"
	m.Age = 100
	m.Right = true
	m.Counter = 1000

	return m
}

func init() {
	st := NewSuite("genmai")
	st.InitF = func() {
		st.AddBenchmark("Insert", 2000*ORM_MULTI, GenmaiInsert)
		st.AddBenchmark("MultiInsert 100 row", 500*ORM_MULTI, GenmaiInsertMulti)
		st.AddBenchmark("Update", 2000*ORM_MULTI, GenmaiUpdate)
		st.AddBenchmark("Read", 4000*ORM_MULTI, GenmaiRead)
		st.AddBenchmark("MultiRead limit 100", 2000*ORM_MULTI, GenmaiReadSlice)

		db, err := genmai.New(&genmai.PostgresDialect{}, ORM_SOURCE)
		checkErr(err)
		genmaidb = db
	}
}

func GenmaiInsert(b *B) {
	var m *GenmaiModel
	wrapExecute(b, func() {
		initDB_genmai()
		m = NewGenmaiModel()
	})

	for i := 0; i < b.N; i++ {
		m.Id = i
		if _, err := genmaidb.Insert(m); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}

func GenmaiInsertMulti(b *B) {
	var ms []GenmaiModel
	wrapExecute(b, func() {
		initDB_genmai()
		ms = make([]GenmaiModel, 0, 100)
		for i := 0; i < 100; i++ {
			m := NewGenmaiModel()
			ms = append(ms, *m)
		}
	})

	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			ms[j].Id = i*100 + j
		}
		if _, err := genmaidb.Insert(ms); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}

func GenmaiUpdate(b *B) {
	var m *GenmaiModel
	wrapExecute(b, func() {
		initDB_genmai()
		m = NewGenmaiModel()
		if _, err := genmaidb.Insert(m); err != nil {
			fmt.Println(err)
			b.FailNow()
		} else {
			m.Id = 1
		}
	})
	for i := 0; i < b.N; i++ {
		_, err := genmaidb.Update(m)
		if err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}

func GenmaiRead(b *B) {
	var m *GenmaiModel
	wrapExecute(b, func() {
		initDB_genmai()
		m = NewGenmaiModel()
		if _, err := genmaidb.Insert(m); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	})
	for i := 0; i < b.N; i++ {
		var results []GenmaiModel
		if err := genmaidb.Select(&results, genmaidb.Where("id", "=", 1)); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}

func GenmaiReadSlice(b *B) {
	var m *GenmaiModel
	wrapExecute(b, func() {
		initDB_genmai()
		m = NewGenmaiModel()
		for i := 0; i < 100; i++ {
			m.Id = 0
			if _, err := genmaidb.Insert(m); err != nil {
				fmt.Println(err)
				b.FailNow()
			}
		}
	})
	for i := 0; i < b.N; i++ {
		var results []GenmaiModel
		if err := genmaidb.Select(&results, genmaidb.Limit(100)); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}
