package csvq

import (
	"context"
	"database/sql"
	"testing"
)

type QueryerContext interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

func TestTx_Commit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeoutForTests)
	defer cancel()

	db, _ := sql.Open("csvq", TestDir)
	defer func() {
		_ = db.Close()
	}()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}

	queryString := "UPDATE `table_txc.csv` SET col2 = 'updated' WHERE col1 = 2"
	result, err := tx.ExecContext(ctx, queryString)
	expectAffectedRows := int64(1)
	if err != nil {
		if err := tx.Rollback(); err != nil {
			t.Log(err)
		}
		t.Fatalf("unexpected error %q", err.Error())
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}
	if affected != expectAffectedRows {
		t.Fatalf("affected rows = %d, want %d", affected, expectAffectedRows)
	}

	queryString = "SELECT INTEGER(col1) AS col1, col2 FROM `table_txc.csv`"
	expectQueryResult := [][]interface{}{
		{1, "str1"},
		{2, "updated"},
		{3, "str3"},
	}
	err = matchRows(ctx, tx, expectQueryResult, queryString)
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Commit()
	if err != nil {
		if err := tx.Rollback(); err != nil {
			t.Log(err)
		}
		t.Fatalf("unexpected error %q", err.Error())
	}

	queryString = "SELECT INTEGER(col1) AS col1, col2 FROM `table_txc.csv`"
	expectQueryResult = [][]interface{}{
		{1, "str1"},
		{2, "updated"},
		{3, "str3"},
	}
	expectErr := sql.ErrTxDone
	err = matchRows(ctx, tx, expectQueryResult, queryString)
	if err == nil {
		t.Fatalf("no error, want error %q", expectErr)
	}
	if err != expectErr {
		t.Fatalf("error = %q, want error %q", err.Error(), expectErr)
	}

	err = matchRows(ctx, db, expectQueryResult, queryString)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTx_Rollback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeoutForTests)
	defer cancel()

	db, _ := sql.Open("csvq", TestDir)
	defer func() {
		_ = db.Close()
	}()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}

	queryString := "UPDATE `table_txr.csv` SET col2 = 'updated' WHERE col1 = 2"
	result, err := tx.ExecContext(ctx, queryString)
	expectAffectedRows := int64(1)
	if err != nil {
		if err := tx.Rollback(); err != nil {
			t.Log(err)
		}
		t.Fatalf("unexpected error %q", err.Error())
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}
	if affected != expectAffectedRows {
		t.Fatalf("affected rows = %d, want %d", affected, expectAffectedRows)
	}

	queryString = "SELECT INTEGER(col1) AS col1, col2 FROM `table_txr.csv`"
	expectQueryResult := [][]interface{}{
		{1, "str1"},
		{2, "updated"},
		{3, "str3"},
	}
	err = matchRows(ctx, tx, expectQueryResult, queryString)
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Rollback()
	if err != nil {
		if err := tx.Rollback(); err != nil {
			t.Log(err)
		}
		t.Fatalf("unexpected error %q", err.Error())
	}

	queryString = "SELECT INTEGER(col1) AS col1, col2 FROM `table_txr.csv`"
	expectQueryResult = [][]interface{}{
		{1, "str1"},
		{2, "str2"},
		{3, "str3"},
	}
	expectErr := sql.ErrTxDone
	err = matchRows(ctx, tx, expectQueryResult, queryString)
	if err == nil {
		t.Fatalf("no error, want error %q", expectErr)
	}
	if err != expectErr {
		t.Fatalf("error = %q, want error %q", err.Error(), expectErr)
	}

	err = matchRows(ctx, db, expectQueryResult, queryString)
	if err != nil {
		t.Fatal(err)
	}
}
