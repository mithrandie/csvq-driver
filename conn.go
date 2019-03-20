package csvq

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"time"

	"github.com/mithrandie/csvq/lib/parser"
	"github.com/mithrandie/csvq/lib/query"
)

type ClosingError struct {
	Errors []error
}

func NewClosingError(errs []error) error {
	return &ClosingError{
		Errors: errs,
	}
}

func (e ClosingError) Error() string {
	list := make([]string, 0, len(e.Errors))
	for _, err := range e.Errors {
		list = append(list, err.Error())
	}
	return strings.Join(list, "\n")
}

type Conn struct {
	dsn                string
	defaultWaitTimeout time.Duration
	retryDelay         time.Duration
	proc               *query.Processor
}

func NewConn(ctx context.Context, dsn string, defaultWaitTimeout time.Duration, retryDelay time.Duration) (*Conn, error) {
	sess := query.NewSession()
	sess.Stdout = &query.Discard{}
	sess.Stderr = &query.Discard{}

	tx, err := query.NewTransaction(ctx, defaultWaitTimeout, retryDelay, sess)
	if err != nil {
		return nil, driver.ErrBadConn
	}

	if err := tx.Flags.SetRepository(dsn); err != nil {
		return nil, driver.ErrBadConn
	}

	proc := query.NewProcessor(tx)
	proc.Tx.AutoCommit = true

	return &Conn{
		dsn:  dsn,
		proc: proc,
	}, nil
}

func (c *Conn) Close() (err error) {
	var errs []error

	if err := c.proc.AutoRollback(); err != nil {
		errs = append(errs, err)
	}
	if err := c.proc.ReleaseResourcesWithErrors(); err != nil {
		errs = append(errs, err)
	}

	if errs != nil {
		err = NewClosingError(errs)
	}
	return
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("csvq does not support prepared statement")
}

func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return NewTx(c.proc)
}

func (c *Conn) QueryContext(ctx context.Context, queryString string, args []driver.NamedValue) (driver.Rows, error) {
	if 0 < len(args) {
		return nil, errors.New("csvq does not support prepared statement")
	}

	if err := c.exec(ctx, queryString, args); err != nil {
		return nil, err
	}
	return NewRows(c.proc.Tx.SelectedViews), nil
}

func (c *Conn) ExecContext(ctx context.Context, queryString string, args []driver.NamedValue) (driver.Result, error) {
	if 0 < len(args) {
		return nil, errors.New("csvq does not support prepared statement")
	}

	if err := c.exec(ctx, queryString, args); err != nil {
		return nil, err
	}
	return NewResult(int64(c.proc.Tx.AffectedRows)), nil
}

func (c *Conn) exec(ctx context.Context, queryString string, args []driver.NamedValue) error {
	statements, err := parser.Parse(queryString, "", c.proc.Tx.Flags.DatetimeFormat)
	if err != nil {
		return query.NewSyntaxError(err.(*parser.SyntaxError))
	}

	_, err = c.proc.Execute(query.ContextForStoringResults(ctx), statements)
	return err
}
