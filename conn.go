package csvq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/mithrandie/csvq/lib/parser"
	"github.com/mithrandie/csvq/lib/query"
)

var session *query.Session
var getSession sync.Once

func GetSession() *query.Session {
	getSession.Do(func() {
		session = query.NewSession()
	})
	return session
}

type CompositeError struct {
	Errors []error
}

func NewCompositeError(errs []error) error {
	return &CompositeError{
		Errors: errs,
	}
}

func (e CompositeError) Error() string {
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
	id                 int
}

func NewConn(ctx context.Context, dsn string, defaultWaitTimeout time.Duration, retryDelay time.Duration) (*Conn, error) {
	sess := GetSession()
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

func (c *Conn) Close() error {
	var errs []error

	if err := c.proc.AutoRollback(); err != nil {
		errs = append(errs, err)
	}
	if err := c.proc.ReleaseResourcesWithErrors(); err != nil {
		errs = append(errs, err)
	}

	var err error
	switch len(errs) {
	case 0:
		//Do nothing
	case 1:
		err = errs[0]
	default:
		err = NewCompositeError(errs)
	}
	return err
}

func (c *Conn) Prepare(queryString string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), queryString)
}

func (c *Conn) PrepareContext(ctx context.Context, queryString string) (driver.Stmt, error) {
	return NewStmt(ctx, c.proc, queryString)
}

func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.Isolation != driver.IsolationLevel(sql.LevelDefault) {
		return nil, errors.New("csvq does not support non-default isolation level")
	}
	if opts.ReadOnly {
		return nil, errors.New("csvq does not support read-only transactions")
	}

	return NewTx(c.proc)
}

func (c *Conn) QueryContext(ctx context.Context, queryString string, args []driver.NamedValue) (driver.Rows, error) {
	if err := c.exec(ctx, queryString, args); err != nil {
		return nil, err
	}
	return NewRows(c.proc.Tx.SelectedViews), nil
}

func (c *Conn) ExecContext(ctx context.Context, queryString string, args []driver.NamedValue) (driver.Result, error) {
	if err := c.exec(ctx, queryString, args); err != nil {
		return nil, err
	}
	return NewResult(int64(c.proc.Tx.AffectedRows)), nil
}

func (c *Conn) exec(ctx context.Context, queryString string, args []driver.NamedValue) error {
	if 0 < len(args) {
		var selectedViews []*query.View
		var affectedRows int

		stmt, err := c.PrepareContext(ctx, queryString)
		if err != nil {
			return err
		}
		defer func() {
			_ = stmt.Close()
			c.proc.Tx.SelectedViews = selectedViews
			c.proc.Tx.AffectedRows = affectedRows
		}()

		err = stmt.(*Stmt).exec(ctx, args)
		if err == nil {
			selectedViews = stmt.(*Stmt).proc.Tx.SelectedViews
			affectedRows = stmt.(*Stmt).proc.Tx.AffectedRows
		}
		return err
	}

	statements, _, err := parser.Parse(queryString, "", c.proc.Tx.Flags.DatetimeFormat, false)
	if err != nil {
		return query.NewSyntaxError(err.(*parser.SyntaxError))
	}

	_, err = c.proc.Execute(query.ContextForStoringResults(ctx), statements)
	return err
}
