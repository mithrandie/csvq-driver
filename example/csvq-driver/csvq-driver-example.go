package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/mithrandie/csvq-driver/example"

	_ "github.com/mithrandie/csvq-driver"
)

var repository = flag.String("r", "", "repository")

func main() {
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := sql.Open("csvq", *repository)
	if err != nil {
		example.ExitWithError(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			example.ExitWithError(err)
		}
	}()
	fmt.Println("######## Opened")

	execPing(ctx, db)
	execSingleRowQueryWithNoRowsError(ctx, db)
	execSingleRowQuery(ctx, db)

	if err := execQuery(ctx, db); err != nil {
		example.ExitWithError(err)
	}

	if err := execMultipleResultSetQuery(ctx, db); err != nil {
		example.ExitWithError(err)
	}

	if err := execPrepared(ctx, db); err != nil {
		example.ExitWithError(err)
	}

	if err := execPreparedWithNamedValue(ctx, db); err != nil {
		example.ExitWithError(err)
	}

	if err := execUpdateInTransaction(ctx, db); err != nil {
		example.ExitWithError(err)
	}

	return
}

func execPing(ctx context.Context, db *sql.DB) {
	fmt.Println("######## Try Ping")
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("connection failed: %s\n", *repository)
	} else {
		fmt.Println("Connected normally.")
	}
}

func execSingleRowQueryWithNoRowsError(ctx context.Context, db *sql.DB) {
	queryString := "SELECT id, first_name, country_code FROM `users.csv` WHERE id = 'notexist'"
	fmt.Printf("\n######## Exec Single Row Query with No Rows Error: %s\n", queryString)

	r := db.QueryRowContext(ctx, queryString)
	example.ScanUser(r)
}

func execSingleRowQuery(ctx context.Context, db *sql.DB) {
	queryString := "SELECT id, first_name, country_code FROM `users.csv` WHERE id = '12'"
	fmt.Printf("\n######## Exec Single Row Query: %s\n", queryString)

	r := db.QueryRowContext(ctx, queryString)
	example.ScanUser(r)
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

	if err := example.ScanUsers(rs); err != nil {
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

	if err := example.ScanUsers(rs); err != nil {
		println(err.Error())
	}

	if rs.NextResultSet() {
		if err := example.ScanUnknownRow(rs); err != nil {
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

	if err := example.ScanUsers(rs); err != nil {
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

	if err := example.ScanUsers(rs); err != nil {
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
	example.ScanUser(r)

	fmt.Println("######## Rollback")
	if err := tx.Rollback(); err != nil {
		println(err.Error())
	}

	return nil
}
