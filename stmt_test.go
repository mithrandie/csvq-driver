package csvq

import (
	"context"
	"database/sql"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// go test -benchmem -run=^$ -bench . github.com/mithrandie/csvq-driver -benchtime=1000x
func BenchmarkQueryOnlyOneTx(b *testing.B) {
	db, _ := sql.Open("csvq", TestDir)
	defer func() {
		_ = db.Close()
	}()

	queryCreate := `create table OnlyOneTx` + strconv.Itoa(b.N) + ` (column1, column2)`
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancelfunc()

	res, err := db.ExecContext(ctx, queryCreate)
	if err != nil {
		b.Logf("Error %s when creating product table", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		b.Logf("Error %s when getting rows affected", err)
		b.Logf("Rows affected when creating table: %d", rows)
	}

	query := `INSERT INTO OnlyOneTx` + strconv.Itoa(b.N) + `(column1, column2) VALUES (?, ?)`

	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		b.Logf("Error %s when preparing SQL statement", err)
	}
	defer stmt.Close()

	// Get a Tx for making transaction requests.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		b.Logf("Error %s tx", err)
	}

	defer tx.Commit()

	// run the Fib function b.N times
	for n := 0; n < b.N; n++ {
		res, err = stmt.ExecContext(ctx, "str"+strconv.Itoa(n), "str"+strconv.Itoa(n))
		if err != nil {
			b.Logf("Error %s when inserting row into table", err)
		} else {
			rows, err = res.RowsAffected()
			if err != nil {
				b.Logf("Error %s when finding rows affected", err)
			}
		}
		// b.Logf("%d row created ", rows)
	}
}

// go test -benchmem -run=^$ -bench ^BenchmarkQueryOnlyOneTxMultipleInsert$ github.com/mithrandie/csvq-driver -benchtime=1000x
func BenchmarkQueryOnlyOneTxMultipleInsert(b *testing.B) {
	db, _ := sql.Open("csvq", TestDir)
	defer func() {
		_ = db.Close()
	}()

	queryCreate := `create table OnlyOneTxMultipleInsert` + strconv.Itoa(b.N) + ` (column1, column2)`
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancelfunc()

	res, err := db.ExecContext(ctx, queryCreate)
	if err != nil {
		b.Logf("Error %s when creating product table", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		b.Logf("Error %s when getting rows affected", err)
		b.Logf("Rows affected when creating table: %d", rows)
	}

	query := `INSERT INTO OnlyOneTxMultipleInsert` + strconv.Itoa(b.N) + `(column1, column2) VALUES (?, ?),(?, ?)`

	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		b.Logf("Error %s when preparing SQL statement", err)
	}
	defer stmt.Close()

	// Get a Tx for making transaction requests.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		b.Logf("Error %s tx", err)
	}

	defer tx.Commit()

	// run the Fib function b.N times
	for n := 0; n < b.N/2; n++ {
		res, err = stmt.ExecContext(ctx, "str"+strconv.Itoa(n), "str"+strconv.Itoa(n), "str"+strconv.Itoa(n), "str"+strconv.Itoa(n))
		if err != nil {
			b.Logf("Error %s when inserting row into table", err)
		} else {
			rows, err = res.RowsAffected()
			if err != nil {
				b.Logf("Error %s when finding rows affected", err)
			}
		}
	}

	// var count int
	// row := db.QueryRow("SELECT COUNT(*) FROM OnlyOneTxMultipleInsert" + strconv.Itoa(b.N))
	// err = row.Scan(&count)
	// if err == nil {
	// 	b.Log(count == b.N)
	// }
}

func BenchmarkQueryTxEveryIter(b *testing.B) {
	db, _ := sql.Open("csvq", TestDir)
	defer func() {
		_ = db.Close()
	}()

	queryCreate := `create table TxEveryIter` + strconv.Itoa(b.N) + ` (column1, column2)`
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancelfunc()

	res, err := db.ExecContext(ctx, queryCreate)
	if err != nil {
		b.Logf("Error %s when creating product table", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		b.Logf("Error %s when getting rows affected", err)
		b.Logf("Rows affected when creating table: %d", rows)
	}

	query := `INSERT INTO TxEveryIter` + strconv.Itoa(b.N) + `(column1, column2) VALUES (?, ?)`

	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		b.Logf("Error %s when preparing SQL statement", err)
	}
	defer stmt.Close()

	// run the Fib function b.N times
	for n := 0; n < b.N; n++ {
		// Get a Tx for making transaction requests.
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			b.Logf("Error %s tx", err)
		}

		res, err = stmt.ExecContext(ctx, "str"+strconv.Itoa(n), "str"+strconv.Itoa(n))
		if err != nil {
			b.Logf("Error %s when inserting row into table", err)
		} else {
			rows, err = res.RowsAffected()
			if err != nil {
				b.Logf("Error %s when finding rows affected", err)
			}
		}

		tx.Commit()
	}
}

func TestStmt_Query(t *testing.T) {
	db, _ := sql.Open("csvq", TestDir)
	defer func() {
		_ = db.Close()
	}()

	queryString := "SELECT FROM `table_sq.csv`"
	expectErr := "[L:1 C:8] syntax error: unexpected token \"FROM\""
	stmt, err := db.Prepare(queryString)
	if err == nil {
		_ = stmt.Close()
		t.Fatalf("no error, want error %q", expectErr)
	}

	queryString = "SELECT INTEGER(col1) AS col1, col2 FROM `table_q.csv` WHERE col1 = ?"
	stmt, err = db.Prepare(queryString)
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}
	defer func() {
		if err := stmt.Close(); err != nil {
			t.Log(err)
		}
	}()

	args := []interface{}{[]int{1, 2}}
	expectErr = "unsupported type: []int"
	rs, err := stmt.Query(args...)
	if err == nil {
		_ = rs.Close()
		t.Fatalf("no error, want error %q", expectErr)
	}
	if !strings.HasSuffix(err.Error(), expectErr) {
		t.Fatalf("error = %q, want error that has suffix %q", err.Error(), expectErr)
	}

	args = []interface{}{1}
	expectResult := [][]interface{}{
		{1, "str1"},
	}
	rs, err = stmt.Query(args...)
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}
	defer func() {
		if err := rs.Close(); err != nil {
			t.Log(err)
		}
	}()
	queryResult, err := scanRows(rs)
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}
	if !reflect.DeepEqual(queryResult, expectResult) {
		t.Fatalf("result = %s, want %s", queryResult, expectResult)
	}

	queryString = "SELECT INTEGER(col1) AS col1, col2 FROM `table_sq.csv` WHERE col1 = :id"
	stmt2, err := db.Prepare(queryString)
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}
	defer func() {
		if err := stmt2.Close(); err != nil {
			t.Log(err)
		}
	}()

	args = []interface{}{sql.Named("id", 2)}
	expectResult = [][]interface{}{
		{2, "str2"},
	}
	rs2, err := stmt2.Query(args...)
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}
	defer func() {
		if err := rs2.Close(); err != nil {
			t.Log(err)
		}
	}()
	queryResult, err = scanRows(rs2)
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}
	if !reflect.DeepEqual(queryResult, expectResult) {
		t.Fatalf("result = %s, want %s", queryResult, expectResult)
	}
}

func TestStmt_Exec(t *testing.T) {
	db, _ := sql.Open("csvq", TestDir)
	defer func() {
		_ = db.Close()
	}()

	queryString := "UPDATE `table_su.csv` SET col2 = 'updated' WHERE col1 = ?"
	stmt, err := db.Prepare(queryString)
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}
	defer func() {
		if err := stmt.Close(); err != nil {
			t.Log(err)
		}
	}()

	args := []interface{}{sql.Named("id", 2)}
	expectAffectedRows := int64(1)
	result, err := stmt.Exec(args...)
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}
	if affected != expectAffectedRows {
		t.Fatalf("affected rows = %d, want %d", affected, expectAffectedRows)
	}

	queryString = "SELECT INTEGER(col1) AS col1, col2 FROM `table_su.csv`"
	expectQueryResult := [][]interface{}{
		{1, "str1"},
		{2, "updated"},
		{3, "str3"},
	}
	rs, _ := db.Query(queryString)
	defer func() {
		if err := rs.Close(); err != nil {
			println(err.Error())
		}
	}()

	queryResult, err := scanRows(rs)
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}
	if !reflect.DeepEqual(queryResult, expectQueryResult) {
		t.Fatalf("result = %s, want %s", queryResult, expectQueryResult)
	}
}
