package csvq

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

var TestDir = filepath.Join(os.TempDir(), "csvq_driver")
var TestDataDir string

var waitTimeoutForTests = 100 * time.Millisecond

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	defer teardown()

	setup()
	return m.Run()
}

func setup() {
	if _, err := os.Stat(TestDir); err == nil {
		_ = os.RemoveAll(TestDir)
	}

	if _, err := os.Stat(TestDir); os.IsNotExist(err) {
		_ = os.Mkdir(TestDir, 0755)
	}

	wd, _ := os.Getwd()
	TestDataDir = filepath.Join(wd, "testdata")

	_ = copyfile(filepath.Join(TestDir, "table_q.csv"), filepath.Join(TestDataDir, "table.csv"))
	_ = copyfile(filepath.Join(TestDir, "table_u.csv"), filepath.Join(TestDataDir, "table.csv"))
	_ = copyfile(filepath.Join(TestDir, "table_sq.csv"), filepath.Join(TestDataDir, "table.csv"))
	_ = copyfile(filepath.Join(TestDir, "table_su.csv"), filepath.Join(TestDataDir, "table.csv"))
	_ = copyfile(filepath.Join(TestDir, "table_txc.csv"), filepath.Join(TestDataDir, "table.csv"))
	_ = copyfile(filepath.Join(TestDir, "table_txr.csv"), filepath.Join(TestDataDir, "table.csv"))
}

func teardown() {
	if _, err := os.Stat(TestDir); err == nil {
		_ = os.RemoveAll(TestDir)
	}
}

func copyfile(dstfile string, srcfile string) error {
	src, err := os.Open(srcfile)
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()

	dst, err := os.Create(dstfile)
	if err != nil {
		return err
	}
	defer func() { _ = dst.Close() }()

	_, err = io.Copy(dst, src)
	if err != nil {
		return err
	}

	return nil
}

func scanRows(rs *sql.Rows) ([][]interface{}, error) {
	var (
		col1 int
		col2 string
	)
	result := make([][]interface{}, 0, 10)
	for rs.Next() {
		if err := rs.Scan(&col1, &col2); err != nil {
			return nil, err
		}
		result = append(result, []interface{}{col1, col2})
	}
	return result, nil
}

func scanRow(r *sql.Row) ([]interface{}, error) {
	var (
		col1 int
		col2 string
	)
	err := r.Scan(&col1, &col2)
	if err != nil {
		return nil, err
	}
	return []interface{}{col1, col2}, nil
}

func matchRows(ctx context.Context, qc QueryerContext, expect [][]interface{}, query string, args ...interface{}) error {
	rs, err := qc.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() {
		if err := rs.Close(); err != nil {
			println(err.Error())
		}
	}()

	queryResult, err := scanRows(rs)
	if err != nil {
		return fmt.Errorf("unexpected error %q", err.Error())
	}
	if !reflect.DeepEqual(queryResult, expect) {
		return fmt.Errorf("result = %s, want %s", queryResult, expect)
	}
	return nil
}
