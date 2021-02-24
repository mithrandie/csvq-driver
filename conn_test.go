package csvq

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	"github.com/mithrandie/csvq/lib/query"
)

func TestConn_BeginTx(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeoutForTests)
	defer cancel()

	db, _ := sql.Open("csvq", TestDir)
	defer func() {
		_ = db.Close()
	}()

	txOptions := &sql.TxOptions{
		Isolation: sql.LevelReadUncommitted,
		ReadOnly:  false,
	}
	expectErr := "csvq does not support non-default isolation level"

	_, err := db.BeginTx(ctx, txOptions)
	if err == nil {
		t.Fatalf("no error, want error %q", expectErr)
	}
	if err.Error() != expectErr {
		t.Fatalf("error = %q, want error %q", err.Error(), expectErr)
	}

	txOptions = &sql.TxOptions{
		Isolation: sql.LevelDefault,
		ReadOnly:  true,
	}
	expectErr = "csvq does not support read-only transactions"

	_, err = db.BeginTx(ctx, txOptions)
	if err == nil {
		t.Fatalf("no error, want error %q", expectErr)
	}
	if err.Error() != expectErr {
		t.Fatalf("error = %q, want error %q", err.Error(), expectErr)
	}

	txOptions = &sql.TxOptions{
		Isolation: sql.LevelDefault,
		ReadOnly:  false,
	}

	tx, err := db.BeginTx(ctx, txOptions)
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}
}

func TestConn_QueryContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeoutForTests)
	defer cancel()

	db, _ := sql.Open("csvq", TestDir)
	defer func() {
		_ = db.Close()
	}()

	queryString := "SELECT FROM `notexist.csv`"
	expectErr := "[L:1 C:8] syntax error: unexpected token \"FROM\""
	err := matchRows(ctx, db, nil, queryString)
	if err == nil {
		t.Fatalf("no error, want error %q", expectErr)
	}
	if _, ok := err.(query.Error); !ok {
		t.Fatal("error type is not a query.Error")
	}
	if err.Error() != expectErr {
		t.Fatalf("error = %q, want error %q", err.Error(), expectErr)
	}

	queryString = "SELECT INTEGER(col1) AS col1, col2 FROM `notexist.csv`"
	expectErr = "[L:1 C:41] file `notexist.csv` does not exist"
	err = matchRows(ctx, db, nil, queryString)
	if err == nil {
		t.Fatalf("no error, want error %q", expectErr)
	}
	if _, ok := err.(query.Error); !ok {
		t.Fatal("error type is not a query.Error")
	}
	if err.Error() != expectErr {
		t.Fatalf("error = %q, want error %q", err.Error(), expectErr)
	}

	queryString = "SELECT INTEGER(col1) AS col1, col2 FROM `table_q.csv`"
	expectColumns := []string{"col1", "col2"}
	expectResult := [][]interface{}{
		{1, "str1"},
		{2, "str2"},
		{3, "str3"},
	}
	rs3, err := db.QueryContext(ctx, queryString)
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}
	defer func() {
		if err := rs3.Close(); err != nil {
			println(err.Error())
		}
	}()

	columns, err := rs3.Columns()
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}
	if !reflect.DeepEqual(columns, expectColumns) {
		t.Fatalf("columns = %s, want %s", columns, expectColumns)
	}

	result, err := scanRows(rs3)
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}
	if !reflect.DeepEqual(result, expectResult) {
		t.Fatalf("result = %s, want %s", result, expectResult)
	}

	queryString = "SELECT INTEGER(col1) AS col1, col2 FROM `table_q.csv` WHERE col1 = 'notexist'"
	expectError := sql.ErrNoRows
	r := db.QueryRowContext(ctx, queryString)
	_, err = scanRow(r)
	if err == nil {
		t.Fatalf("no error, want error %q", expectErr)
	}
	if err != expectError {
		t.Fatalf("error = %q, want error %q", err, expectError)
	}

	queryString = "SELECT INTEGER(col1) AS col1, col2 FROM `table_q.csv` WHERE col1 = 2"
	expectRowResult := []interface{}{2, "str2"}
	r2 := db.QueryRowContext(ctx, queryString)
	rowResult, err := scanRow(r2)
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}
	if !reflect.DeepEqual(rowResult, expectRowResult) {
		t.Fatalf("result = %s, want %s", rowResult, expectRowResult)
	}

	queryString = "SELECT INTEGER(col1) AS col1, col2 FROM `table_q.csv` WHERE col1 = ?"
	args := []interface{}{1}
	expectResult = [][]interface{}{
		{1, "str1"},
	}
	err = matchRows(ctx, db, expectResult, queryString, args...)
	if err != nil {
		t.Fatal(err)
	}
}

func TestConn_ExecContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeoutForTests)
	defer cancel()

	db, _ := sql.Open("csvq", TestDir)
	defer func() {
		_ = db.Close()
	}()

	queryString := "UPDATE `table_u.csv` WHERE col1 = 2"
	expectErr := "[L:1 C:22] syntax error: unexpected token \"WHERE\""
	result, err := db.ExecContext(ctx, queryString)
	if err == nil {
		t.Fatalf("no error, want error %q", expectErr)
	}
	if _, ok := err.(query.Error); !ok {
		t.Fatal("error type is not a query.Error")
	}
	if err.Error() != expectErr {
		t.Fatalf("error = %q, want error %q", err.Error(), expectErr)
	}

	queryString = "UPDATE `table_u.csv` SET col2 = 'updated' WHERE col1 = 2"
	expectLastInsertIdErr := "csvq does not support LastInsertId()"
	expectAffectedRows := int64(1)
	result, err = db.ExecContext(ctx, queryString)
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}
	_, err = result.LastInsertId()
	if err == nil {
		t.Fatalf("no error, want error %q", expectLastInsertIdErr)
	}
	if err.Error() != expectLastInsertIdErr {
		t.Fatalf("error = %q, want error %q", err.Error(), expectLastInsertIdErr)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}
	if affected != expectAffectedRows {
		t.Fatalf("affected rows = %d, want %d", affected, expectAffectedRows)
	}

	queryString = "SELECT INTEGER(col1) AS col1, col2 FROM `table_u.csv`"
	expectQueryResult := [][]interface{}{
		{1, "str1"},
		{2, "updated"},
		{3, "str3"},
	}
	err = matchRows(ctx, db, expectQueryResult, queryString)
	if err != nil {
		t.Fatal(err)
	}
}

var parseDSNTests = []struct {
	DSN      string
	Result   DSN
	HasError bool
}{
	{
		DSN: "/path/to/data/directory",
		Result: DSN{
			repository:     "/path/to/data/directory",
			timezone:       "Local",
			datetimeFormat: "",
			ansiQuotes:     false,
		},
		HasError: false,
	},
	{
		DSN: "/path/to/data/directory?",
		Result: DSN{
			repository:     "/path/to/data/directory",
			timezone:       "Local",
			datetimeFormat: "",
			ansiQuotes:     false,
		},
		HasError: false,
	},
	{
		DSN: "/path/to/data/directory?Timezone=UTC&DatetimeFormat=[\"%d%m%Y\"]&AnsiQuotes=true",
		Result: DSN{
			repository:     "/path/to/data/directory",
			timezone:       "UTC",
			datetimeFormat: "[\"%d%m%Y\"]",
			ansiQuotes:     true,
		},
		HasError: false,
	},
	{
		DSN: "/path/to/data/directory?timezone=UTC&datetimeformat=[\"%d%m%Y\"]&ansiquotes=true",
		Result: DSN{
			repository:     "/path/to/data/directory",
			timezone:       "UTC",
			datetimeFormat: "[\"%d%m%Y\"]",
			ansiQuotes:     true,
		},
		HasError: false,
	},
	{
		DSN: "/path/to/data/directory?datetimeformat=[\"?%d%m\\\"%Y=&\"]",
		Result: DSN{
			repository:     "/path/to/data/directory",
			timezone:       "Local",
			datetimeFormat: "[\"?%d%m\\\"%Y=&\"]",
			ansiQuotes:     false,
		},
		HasError: false,
	},
	{
		DSN: "/path/to/data/directory?timezone&datetimeformat&ansiquotes",
		Result: DSN{
			repository:     "/path/to/data/directory",
			timezone:       "Local",
			datetimeFormat: "",
			ansiQuotes:     false,
		},
		HasError: false,
	},
	{
		DSN: "/path/to/data/directory?timezone&datetimeformat&ansiquotes=true&",
		Result: DSN{
			repository:     "/path/to/data/directory",
			timezone:       "Local",
			datetimeFormat: "",
			ansiQuotes:     true,
		},
		HasError: false,
	},
	{
		DSN:      "/path/to/data/directory?timezone&datetimeformat&ansiquotes=err&",
		HasError: true,
	},
	{
		DSN:      "/path/to/data/directory?Timezone=UTC&IncorrectParam=true",
		HasError: true,
	},
}

func TestParseDSN(t *testing.T) {
	for _, v := range parseDSNTests {
		result, err := ParseDSN(v.DSN)
		if v.HasError {
			if err == nil {
				t.Errorf("%s: no error has returned", v.DSN)
			}
			continue
		}

		if err != nil {
			t.Errorf("%s: unexpected error", v.DSN)
			continue
		}

		if !reflect.DeepEqual(result, v.Result) {
			t.Errorf("%s: DSN is %v, want %v", v.DSN, result, v.Result)
		}
	}
}
