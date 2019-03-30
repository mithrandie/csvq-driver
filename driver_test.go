package csvq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"path/filepath"
	"testing"
)

func TestDriver_Open(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeoutForTests)
	defer cancel()

	db, _ := sql.Open("csvq", filepath.Join(TestDir, "notexistdir"))
	defer func() {
		_ = db.Close()
	}()

	expectErr := driver.ErrBadConn
	err := db.PingContext(ctx)
	if err == nil {
		t.Fatalf("no error, want error %q", expectErr)
	}
	if err != expectErr {
		t.Fatalf("error = %q, want error %q", err.Error(), expectErr)
	}

	db2, _ := sql.Open("csvq", TestDir)
	defer func() {
		_ = db2.Close()
	}()

	err = db2.PingContext(ctx)
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}

	err = db2.Close()
	if err != nil {
		t.Fatalf("unexpected error %q", err.Error())
	}
}
