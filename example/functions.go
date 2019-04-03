package example

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/mithrandie/csvq/lib/query"
)

func ExitWithError(err error) {
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

func ScanUser(r *sql.Row) {
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

func ScanUsers(rs *sql.Rows) error {
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

func ScanUnknownRow(rs *sql.Rows) error {
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
