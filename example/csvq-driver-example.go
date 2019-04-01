package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/mithrandie/csvq/lib/query"

	_ "github.com/mithrandie/csvq-driver"
)

var repository = flag.String("r", "", "repository")

func main() {
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := sql.Open("csvq", *repository)
	if err != nil {
		exitWithError(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			exitWithError(err)
		}
	}()
	fmt.Println("######## Opened")

	execPing(ctx, db)
	execSingleRowQueryWithNoRowsError(ctx, db)
	execSingleRowQuery(ctx, db)

	if err := execQuery(ctx, db); err != nil {
		exitWithError(err)
	}

	if err := execMultipleResultSetQuery(ctx, db); err != nil {
		exitWithError(err)
	}

	if err := execPrepared(ctx, db); err != nil {
		exitWithError(err)
	}

	if err := execPreparedWithNamedValue(ctx, db); err != nil {
		exitWithError(err)
	}

	if err := execUpdateInTransaction(ctx, db); err != nil {
		exitWithError(err)
	}

	return
}

func exitWithError(err error) {
	if queryErr, ok := err.(query.Error); ok {
		log.Fatalf("Error Code:%d, Number:%d, Line:%d, Char:%d, Message:%s",
			queryErr.Code(),
			queryErr.Number(),
			queryErr.Line(),
			queryErr.Char(),
			queryErr.Message(),
		)
	} else {
		log.Fatal(err.Error())
	}
}

func execPing(ctx context.Context, db *sql.DB) {
	fmt.Println("######## Try Ping")
	if err := db.PingContext(ctx); err != nil {
		panic(fmt.Sprintf("connection failed: %s\n", *repository))
	} else {
		fmt.Println("Connected normally.")
	}
}

func execSingleRowQueryWithNoRowsError(ctx context.Context, db *sql.DB) {
	queryString := "SELECT id, first_name, country_code FROM `users.csv` WHERE id = 'notexist'"
	fmt.Printf("\n######## Exec Single Row Query with No Rows Error: %s\n", queryString)

	r := db.QueryRowContext(ctx, queryString)
	scanUser(r)
}

func execSingleRowQuery(ctx context.Context, db *sql.DB) {
	queryString := "SELECT id, first_name, country_code FROM `users.csv` WHERE id = '12'"
	fmt.Printf("\n######## Exec Single Row Query: %s\n", queryString)

	r := db.QueryRowContext(ctx, queryString)
	scanUser(r)
}

func execQuery(ctx context.Context, db *sql.DB) error {
	queryString := "SELECT id, first_name, country_code FROM `users.csv` WHERE country_code = 'PL' LIMIT 3"
	fmt.Printf("\n######## Exec Query: \n%s\n", queryString)

	rs, err := db.QueryContext(ctx, queryString)
	if err != nil {
		return err
	}

	defer func() {
		if err := rs.Close(); err != nil {
			println(err.Error())
		}
	}()

	if err := scanUsers(rs); err != nil {
		println(err.Error())
	}

	return nil
}

func execMultipleResultSetQuery(ctx context.Context, db *sql.DB) error {
	queryString := "" +
		"SELECT id, first_name, country_code FROM `users.csv` WHERE country_code = 'PL' LIMIT 3;\n" +
		"SELECT * FROM `users.csv` WHERE country_code = 'AR' LIMIT 3;"
	fmt.Printf("\n######## Exec Multiple-Result-Set Query: %s\n", queryString)

	rs, err := db.QueryContext(ctx, queryString)
	if err != nil {
		return err
	}

	defer func() {
		if err := rs.Close(); err != nil {
			println(err.Error())
		}
	}()

	defer func() {
		if err := rs.Close(); err != nil {
			println(err.Error())
		}
	}()

	if err := scanUsers(rs); err != nil {
		println(err.Error())
	}

	if rs.NextResultSet() {
		if err := scanUnknownRow(rs); err != nil {
			println(err.Error())
		}
	}
	return nil
}

func execPrepared(ctx context.Context, db *sql.DB) error {
	queryString := "SELECT id, first_name, country_code FROM `users.csv` WHERE country_code = ? LIMIT 3"
	fmt.Printf("\n######## Exec Prepared: %s\n", queryString)

	stmt, err := db.Prepare(queryString)
	if err != nil {
		return err
	}

	//Try incorrect arguments
	fmt.Printf("**** Try with no args.\n")
	rs, err := stmt.QueryContext(ctx)
	if err != nil {
		println("Error: " + err.Error())
	}

	args := []interface{}{"PL"}
	fmt.Printf("**** Try with args: %v\n", args)
	rs, err = stmt.QueryContext(ctx, args...)
	if err != nil {
		return err
	}

	defer func() {
		if err := rs.Close(); err != nil {
			println(err.Error())
		}
	}()

	if err := scanUsers(rs); err != nil {
		println(err.Error())
	}

	return nil
}

func execPreparedWithNamedValue(ctx context.Context, db *sql.DB) error {
	queryString := "SELECT id, first_name, country_code FROM `users.csv` WHERE country_code = :country_code LIMIT 3"
	fmt.Printf("\n######## Exec Prepared with NamedValue: %s\n", queryString)

	stmt, err := db.Prepare(queryString)
	if err != nil {
		return err
	}

	args := []interface{}{sql.Named("country_code", "PL")}
	fmt.Printf("**** Try with args: %v\n", args)
	rs, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return err
	}

	defer func() {
		if err := rs.Close(); err != nil {
			println(err.Error())
		}
	}()

	if err := scanUsers(rs); err != nil {
		println(err.Error())
	}
	return nil
}

func execUpdateInTransaction(ctx context.Context, db *sql.DB) error {
	fmt.Println("\n######## Begin Transaction")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	queryString := "UPDATE `users.csv` SET first_name = 'foo' WHERE id = 12"
	fmt.Printf("**** Run Update Query: %s\n", queryString)
	ret, err := tx.ExecContext(ctx, queryString)
	if err != nil {
		if e := tx.Rollback(); e != nil {
			println(e.Error())
		}
		return err
	}

	_, err = ret.LastInsertId()
	if err != nil {
		fmt.Printf("LastInsertId: %s\n", err.Error())
	}
	affected, _ := ret.RowsAffected()
	fmt.Printf("RowsAffected: %d\n", affected)

	queryString = "SELECT id, first_name, country_code FROM `users.csv` WHERE id = '12'"
	fmt.Printf("**** Retrieve Updated Row in the Transaction: %s\n", queryString)
	r := tx.QueryRowContext(ctx, queryString)
	scanUser(r)

	fmt.Println("######## Rollback")
	if err := tx.Rollback(); err != nil {
		println(err.Error())
	}

	return nil
}

func scanUser(r *sql.Row) {
	var (
		id          int
		firstName   string
		countryCode string
	)

	if err := r.Scan(&id, &firstName, &countryCode); err != nil {
		if err == sql.ErrNoRows {
			fmt.Println("No Rows.")
		} else {
			println("Unexpected error: " + err.Error())
		}
		return
	}
	fmt.Printf("Result: [id]%3d  [first_name]%10s  [country_code]%3s\n", id, firstName, countryCode)
}

func scanUsers(rs *sql.Rows) error {
	var (
		id          int
		firstName   string
		countryCode string
	)

	for rs.Next() {
		if err := rs.Scan(&id, &firstName, &countryCode); err != nil {
			return err
		}
		fmt.Printf("Result: [id]%3d  [first_name]%10s  [country_code]%3s\n", id, firstName, countryCode)
	}
	return nil
}

func scanUnknownRow(rs *sql.Rows) error {
	columns, err := rs.Columns()
	if err != nil {
		return err
	}

	var row = make([]interface{}, 0, len(columns))
	for range columns {
		row = append(row, new(string))
	}

	for rs.Next() {
		if err := rs.Scan(row...); err != nil {
			return err
		}
		for i := range columns {
			if 0 < i {
				fmt.Print("  ")
			}
			fmt.Printf("%s:%s", columns[i], *row[i].(*string))
		}
		fmt.Print("\n")
	}
	return nil
}
