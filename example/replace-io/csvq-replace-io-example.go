// Usage:
//   $ cd $GOPATH/src/github.com/mithrandie/csvq-driver
//   $ go build ./example/csvq-driver/csvq-replace-io-example.go
//   $ ./csvq-replace-io-example
//
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/mithrandie/csvq/lib/query"

	"github.com/mithrandie/csvq-driver"
	"github.com/mithrandie/csvq-driver/example"
)

func main() {
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := sql.Open("csvq", "")
	if err != nil {
		example.ExitWithError(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			example.ExitWithError(err)
		}
	}()
	fmt.Println("######## Opened")

	fmt.Printf("\n######## Replace I/O files\n")
	data := `
[
  {
    "id": 1,
    "first_name": "Louis",
    "country_code": "PS"
  },
  {
    "id": 2,
    "first_name": "Sean",
    "country_code": "SE"
  },
  {
    "id": 3,
    "first_name": "Mildred",
    "country_code": "ID"
  }
]`

	stdin := query.NewInput(strings.NewReader(data))
	stdout := query.NewOutput()
	outfile := query.NewOutput()
	if err := csvq.SetStdin(stdin); err != nil {
		panic(err)
	}
	csvq.SetStdout(stdout)
	csvq.SetOutFile(outfile)

	queryString := "" +
		"SET @@IMPORT_FORMAT TO JSON;" +
		"SET @@FORMAT TO JSON;" +
		"SET @@PRETTY_PRINT TO TRUE;"
	fmt.Printf("\n######## Exec Set Flag Statements: %s\n", queryString)
	_, err = db.ExecContext(ctx, queryString)
	if err != nil {
		example.ExitWithError(err)
	}

	queryString = "UPDATE stdin SET first_name = 'foo' WHERE id = 2;"
	fmt.Printf("\n######## Exec Update Query: %s\n", queryString)
	ret, err := db.ExecContext(ctx, queryString)
	if err != nil {
		example.ExitWithError(err)
	}
	affected, _ := ret.RowsAffected()
	fmt.Printf("RowsAffected: %d\n", affected)

	queryString = "SELECT id, first_name, country_code FROM stdin;"
	fmt.Printf("\n######## Exec Select Query: %s\n", queryString)
	rs, err := db.QueryContext(ctx, queryString)
	if err != nil {
		example.ExitWithError(err)
	}
	defer func() {
		if err := rs.Close(); err != nil {
			println(err.Error())
		}
	}()
	if err := example.ScanUsers(rs); err != nil {
		println(err.Error())
	}

	fmt.Println("\n######## Print Stdout")
	fmt.Println(stdout.String())
	fmt.Println("\n######## Print OutFile")
	fmt.Println(outfile.String())
}
