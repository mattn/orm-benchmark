package benchs

//go:generate go run -mod=mod entgo.io/ent/cmd/ent init User

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/mattn/orm-benchmark/benchs/ent"
	//"github.com/mattn/orm-benchmark/benchs/ent/model"
)

var entClient *ent.Client

func initDB_ent() {

	sqls := []string{
		`DROP TABLE IF EXISTS model;`,
		`CREATE TABLE model (
		id integer NOT NULL,
		name text NOT NULL,
		title text NOT NULL,
		fax text NOT NULL,
		web text NOT NULL,
		age integer NOT NULL,
		"right" boolean NOT NULL,
		counter bigint NOT NULL
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

func init() {
	st := NewSuite("ent")
	st.InitF = func() {
		st.AddBenchmark("Insert", 2000*ORM_MULTI, EntInsert)
		st.AddBenchmark("MultiInsert 100 row", 500*ORM_MULTI, EntInsertMulti)
		st.AddBenchmark("Update", 2000*ORM_MULTI, EntUpdate)
		st.AddBenchmark("Read", 4000*ORM_MULTI, EntRead)
		st.AddBenchmark("MultiRead limit 100", 2000*ORM_MULTI, EntReadSlice)

		client, err := ent.Open("postgres", ORM_SOURCE)
		checkErr(err)

		entClient = client
	}
}

func EntInsert(b *B) {
	var m *ent.ModelCreate
	wrapExecute(b, func() {
		initDB_ent()
		m = entClient.Model.
			Create().
			SetName("Orm Benchmark").
			SetTitle("Just a Benchmark for fun").
			SetFax("99909990").
			SetWeb("http://blog.milkpod29.me").
			SetAge(100).
			SetRight(true).
			SetCounter(1000)
	})

	for i := 0; i < b.N; i++ {
		if _, err := m.Save(context.Background()); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}

func EntInsertMulti(b *B) {
	var ms []*ent.ModelCreate
	wrapExecute(b, func() {
		initDB_ent()
		ms = make([]*ent.ModelCreate, 0, 100)
		for i := 0; i < 100; i++ {
			ms = append(ms, entClient.Model.
				Create().
				SetName("Orm Benchmark").
				SetTitle("Just a Benchmark for fun").
				SetFax("99909990").
				SetWeb("http://blog.milkpod29.me").
				SetAge(100).
				SetRight(true).
				SetCounter(1000))
		}
	})
	for i := 0; i < b.N; i++ {
		if _, err := entClient.Model.CreateBulk(ms...).Save(context.Background()); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}

func EntUpdate(b *B) {
	var m *ent.ModelUpdateOne
	wrapExecute(b, func() {
		initDB_ent()
		i := entClient.Model.
			Create().
			SetName("Orm Benchmark").
			SetTitle("Just a Benchmark for fun").
			SetFax("99909990").
			SetWeb("http://blog.milkpod29.me").
			SetAge(100).
			SetRight(true).
			SetCounter(1000)

		mm, err := i.Save(context.Background())
		if err != nil {
			fmt.Println(err)
			b.FailNow()
		}

		m = entClient.Model.UpdateOne(mm)
	})

	for i := 0; i < b.N; i++ {
		if _, err := m.Save(context.Background()); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}

func EntRead(b *B) {
	wrapExecute(b, func() {
		initDB_ent()
		i := entClient.Model.
			Create().
			SetName("Orm Benchmark").
			SetTitle("Just a Benchmark for fun").
			SetFax("99909990").
			SetWeb("http://blog.milkpod29.me").
			SetAge(100).
			SetRight(true).
			SetCounter(1000)

		_, err := i.Save(context.Background())
		checkErr(err)
	})

	for i := 0; i < b.N; i++ {
		if _, err := entClient.Model.Get(context.Background(), 1); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}

func EntReadSlice(b *B) {
	wrapExecute(b, func() {
		initDB_ent()
		for i := 0; i < 100; i++ {
			i := entClient.Model.
				Create().
				SetName("Orm Benchmark").
				SetTitle("Just a Benchmark for fun").
				SetFax("99909990").
				SetWeb("http://blog.milkpod29.me").
				SetAge(100).
				SetRight(true).
				SetCounter(1000)
			_, err := i.Save(context.Background())
			checkErr(err)
		}
	})

	query := entClient.Model.Query().Limit(100)
	for i := 0; i < b.N; i++ {
		if _, err := query.All(context.Background()); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}
