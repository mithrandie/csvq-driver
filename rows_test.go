package csvq

import (
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/mithrandie/csvq/lib/query"
	"github.com/mithrandie/csvq/lib/value"

	"github.com/mithrandie/ternary"
)

var UTC = func() *time.Location {
	utc, _ := time.LoadLocation("UTC")
	return utc
}()

var (
	testEmptySelectedViews = []*query.View(nil)
	testSelectedViews      = []*query.View{
		{
			Header: query.NewHeader("table1", []string{"col1", "col2", "col3", "col4", "col5", "col6", "col7"}),
			RecordSet: query.RecordSet{
				query.NewRecord([]value.Primary{
					value.NewString("abc"),
					value.NewInteger(123),
					value.NewFloat(1.234),
					value.NewBoolean(true),
					value.NewTernary(ternary.UNKNOWN),
					value.NewDatetimeFromString("2012-02-01T12:35:43Z", nil, UTC),
					value.NewNull(),
				}),
				query.NewRecord([]value.Primary{
					value.NewString("efg"),
					value.NewInteger(456),
					value.NewFloat(5.678),
					value.NewBoolean(false),
					value.NewTernary(ternary.TRUE),
					value.NewDatetimeFromString("2012-02-01T12:35:43Z", nil, UTC),
					value.NewNull(),
				}),
			},
		},
		{
			Header: query.NewHeader("table2", []string{"col1", "col2", "col3"}),
			RecordSet: query.RecordSet{
				query.NewRecord([]value.Primary{
					value.NewString("abc"),
					value.NewInteger(123),
					value.NewFloat(1.234),
				}),
			},
		},
	}
)

func TestRows_Columns(t *testing.T) {
	rows := NewRows(testEmptySelectedViews)
	defer func() {
		_ = rows.Close()
	}()
	var expect []string = nil

	result := rows.Columns()
	if !reflect.DeepEqual(result, expect) {
		t.Errorf("result = %q, want %q", result, expect)
	}

	rows2 := NewRows(testSelectedViews)
	defer func() {
		_ = rows2.Close()
	}()
	expect = []string{"col1", "col2", "col3", "col4", "col5", "col6", "col7"}

	result = rows2.Columns()
	if !reflect.DeepEqual(result, expect) {
		t.Errorf("result = %q, want %q", result, expect)
	}
}

func TestRows_Next(t *testing.T) {
	rows := NewRows(testEmptySelectedViews)
	defer func() {
		_ = rows.Close()
	}()
	var expectErr = io.EOF

	result := make([]driver.Value, 5)
	err := rows.Next(result)
	if err == nil {
		t.Fatalf("no error, want error %q", expectErr)
	}
	if err != expectErr {
		t.Fatalf("error = %q, want error %q", err, expectErr)
	}

	rows2 := NewRows(testSelectedViews)
	defer func() {
		_ = rows2.Close()
	}()

	expectErr = errors.New("column length does not match")
	err = rows2.Next(result)
	if err == nil {
		t.Fatalf("no error, want error %q", expectErr)
	}
	if err.Error() != expectErr.Error() {
		t.Fatalf("error = %q, want error %q", err, expectErr)
	}

	expect := []driver.Value{
		"abc",
		int64(123),
		float64(1.234),
		true,
		nil,
		time.Date(2012, 2, 1, 12, 35, 43, 0, time.UTC),
		nil,
	}
	result = make([]driver.Value, 7)
	err = rows2.Next(result)
	if err != nil {
		t.Fatalf("unexpected error %q", err)
	}
	if !reflect.DeepEqual(result, expect) {
		t.Fatalf("result = %s, want %s", result, expect)
	}

	expect = []driver.Value{
		"efg",
		int64(456),
		float64(5.678),
		false,
		true,
		time.Date(2012, 2, 1, 12, 35, 43, 0, time.UTC),
		nil,
	}
	err = rows2.Next(result)
	if err != nil {
		t.Fatalf("unexpected error %q", err)
	}
	if !reflect.DeepEqual(result, expect) {
		t.Fatalf("result = %s, want %s", result, expect)
	}

	expectErr = io.EOF
	err = rows2.Next(result)
	if err == nil {
		t.Fatalf("no error, want error %q", expectErr)
	}
	if err != expectErr {
		t.Fatalf("error = %q, want error %q", err, expectErr)
	}

	var expectBool bool
	expectBool = true
	resultBool := rows2.HasNextResultSet()
	if resultBool != expectBool {
		t.Fatalf("result of HasNextResultSet = %t, want %t", resultBool, expectBool)
	}

	err = rows2.NextResultSet()
	if err != nil {
		t.Fatalf("unexpected error %q", err)
	}

	expect = []driver.Value{
		"abc",
		int64(123),
		float64(1.234),
	}
	result = make([]driver.Value, 3)
	err = rows2.Next(result)
	if err != nil {
		t.Fatalf("unexpected error %q", err)
	}
	if !reflect.DeepEqual(result, expect) {
		t.Fatalf("result = %s, want %s", result, expect)
	}

	expectErr = io.EOF
	err = rows2.Next(result)
	if err == nil {
		t.Fatalf("no error, want error %q", expectErr)
	}
	if err != expectErr {
		t.Fatalf("error = %q, want error %q", err, expectErr)
	}

	expectBool = false
	resultBool = rows2.HasNextResultSet()
	if resultBool != expectBool {
		t.Fatalf("result of HasNextResultSet = %t, want %t", resultBool, expectBool)
	}

	expectErr = io.EOF
	err = rows2.NextResultSet()
	if err == nil {
		t.Fatalf("no error, want error %q", expectErr)
	}
	if err != expectErr {
		t.Fatalf("error = %q, want error %q", err, expectErr)
	}
}
