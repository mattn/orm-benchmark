package benchs

import (
	"context"
	"fmt"

	"database/sql"

	_ "github.com/lib/pq"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

var bundb *bun.DB

type BunModel struct {
	bun.BaseModel `bun:"table:model,alias:m"`

	Id int64 `bun:"id,pk,autoincrement"`

	Name    string `bun:"name,notnull"`
	Title   string `bun:"title,notnull"`
	Fax     string `bun:"fax,notnull"`
	Web     string `bun:"web,notnull"`
	Age     int    `bun:"age,notnull"`
	Right   bool   `bun:"right,notnull"`
	Counter int64  `bun:"counter,notnull"`
}

func NewBunModel() *BunModel {
	m := new(BunModel)
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
	st := NewSuite("bun")
	st.InitF = func() {
		st.AddBenchmark("Insert", 2000*ORM_MULTI, BunInsert)
		st.AddBenchmark("MultiInsert 100 row", 500*ORM_MULTI, BunInsertMulti)
		st.AddBenchmark("Update", 2000*ORM_MULTI, BunUpdate)
		st.AddBenchmark("Read", 4000*ORM_MULTI, BunRead)
		st.AddBenchmark("MultiRead limit 100", 2000*ORM_MULTI, BunReadSlice)

		db, err := sql.Open("postgres", ORM_SOURCE)
		checkErr(err)
		bundb = bun.NewDB(db, pgdialect.New())
	}
}

func BunInsert(b *B) {
	var m *Model
	wrapExecute(b, func() {
		initDB()
		m = NewModel()
	})
	for i := 0; i < b.N; i++ {
		m.Id = 0
		_, err := bundb.NewInsert().Model(m).Exec(context.Background())
		if err != nil {
			fmt.Println(err.Error())
			b.FailNow()
		}
	}
}

func BunInsertMulti(b *B) {
	var ms []*BunModel
	wrapExecute(b, func() {
		initDB3()
		ms = make([]*BunModel, 0, 100)
		for i := 0; i < 100; i++ {
			ms = append(ms, NewBunModel())
		}
	})

	for i := 0; i < b.N; i++ {
		if _, err := bundb.NewInsert().Model(ms).Exec(context.Background()); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}

func BunUpdate(b *B) {
	var m *BunModel
	wrapExecute(b, func() {
		initDB3()
		m = NewBunModel()
		if _, err := bundb.NewInsert().Model(m).Exec(context.Background()); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	})

	for i := 0; i < b.N; i++ {
		if _, err := bo.Update(m); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}

func BunRead(b *B) {
	var m *BunModel
	wrapExecute(b, func() {
		initDB3()
		m = NewBunModel()
		if _, err := bundb.NewInsert().Model(m).Exec(context.Background()); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	})

	for i := 0; i < b.N; i++ {
		if err := bundb.NewSelect().Model(m).Scan(context.Background(), m); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}

func BunReadSlice(b *B) {
	var m *BunModel
	wrapExecute(b, func() {
		initDB3()
		m = NewBunModel()
		for i := 0; i < 100; i++ {
			m.Id = 0
			if _, err := bundb.NewInsert().Model(m).Exec(context.Background()); err != nil {
				fmt.Println(err)
				b.FailNow()
			}
		}
	})

	for i := 0; i < b.N; i++ {
		var models []BunModel
		if err := bundb.NewSelect().Model(models).Where("id > ?", 0).Limit(100).Scan(context.Background()); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}
