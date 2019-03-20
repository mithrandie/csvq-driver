package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"time"

	_ "github.com/mithrandie/csvq-driver"
)

var repository = flag.String("r", "", "repository")

func main() {
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := sql.Open("csvq", *repository)
	if err != nil {
		panic(err.Error())
	}
	defer func() {
		if err := db.Close(); err != nil {
			panic(err.Error())
		}
	}()
	fmt.Println("######## Opend")

	fmt.Println("\n######## Ping")
	if err := db.PingContext(ctx); err != nil {
		panic(fmt.Sprintf("connection failed: %s\n", *repository))
	}

	fmt.Println("\n######## Run Multiple Select Query")
	var (
		id          int
		firstName   string
		countryCode string
	)

	//rs, err := db.QueryContext(ctx, "SELECT INTEGER(id) AS id, first_name, country_code FROM `users.csv` WHERE country_code = 'ID'")
	query := "" +
		"SELECT INTEGER(id) AS id, first_name, country_code FROM `users.csv` WHERE country_code = 'ID';" +
		"SELECT * FROM `users.csv` WHERE country_code = 'AR';"
	rs, err := db.QueryContext(ctx, query)
	if err != nil {
		panic(err.Error())
	}

	defer func() {
		if err := rs.Close(); err != nil {
			println(err.Error())
		}
	}()

	for rs.Next() {
		if err := rs.Scan(&id, &firstName, &countryCode); err != nil {
			println(err.Error())
			break
		}
		fmt.Printf("[id]%3d  [first_name]%10s  [country_code]%3s\n", id, firstName, countryCode)
	}

	if rs.NextResultSet() {
		columns, err := rs.Columns()
		if err != nil {
			println(err.Error())
		} else {
			var row = make([]interface{}, 0, len(columns))
			for range columns {
				row = append(row, new(string))
			}

			for rs.Next() {
				if err := rs.Scan(row...); err != nil {
					println(err.Error())
					break
				}
				for i := range columns {
					if 0 < i {
						fmt.Print("  ")
					}
					fmt.Printf("%s:%s", columns[i], *row[i].(*string))
				}
				fmt.Print("\n")
			}
		}
	}

	fmt.Println("\n######## Begin Transaction")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		panic(err.Error())
	}

	fmt.Println("\n######## Run Update Query")
	ret, err := tx.ExecContext(ctx, "UPDATE `users.csv` SET first_name = 'foo' WHERE id = 3")
	if err != nil {
		println(err.Error())

		if err := tx.Rollback(); err != nil {
			println(err.Error())
		}
	} else {
		affected, _ := ret.RowsAffected()
		fmt.Printf("RowsAffected: %d\n", affected)

		rs2, err := tx.QueryContext(ctx, "SELECT INTEGER(id) AS id, first_name, country_code FROM `users.csv` WHERE country_code = 'ID' LIMIT 3")
		if err != nil {
			panic(err.Error())
		}

		defer func() {
			if err := rs2.Close(); err != nil {
				println(err.Error())
			}
		}()

		for rs2.Next() {
			if err := rs2.Scan(&id, &firstName, &countryCode); err != nil {
				println(err.Error())
				break
			}
			fmt.Printf("[id]%3d  [first_name]%10s  [country_code]%3s\n", id, firstName, countryCode)
		}

		if err := tx.Rollback(); err != nil {
			println(err.Error())
		}
		//if err := tx.Commit(); err != nil {
		//	println(err.Error())
		//}
	}
}
