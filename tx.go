package csvq

import (
	"database/sql/driver"

	"github.com/mithrandie/csvq/lib/parser"

	"github.com/mithrandie/csvq/lib/query"
)

type Tx struct {
	proc *query.Processor
}

func NewTx(proc *query.Processor) (driver.Tx, error) {
	proc.Tx.AutoCommit = false

	return &Tx{
		proc: proc,
	}, nil
}

func (tx Tx) Commit() error {
	token := parser.Token{
		Token:   parser.COMMIT,
		Literal: "COMMIT",
		Line:    1,
		Char:    1,
	}
	expr := parser.TransactionControl{BaseExpr: parser.NewBaseExpr(token), Token: parser.COMMIT}
	return tx.proc.Commit(expr)
}

func (tx Tx) Rollback() error {
	token := parser.Token{
		Token:   parser.ROLLBACK,
		Literal: "ROLLBACK",
		Line:    1,
		Char:    1,
	}
	expr := parser.TransactionControl{BaseExpr: parser.NewBaseExpr(token), Token: parser.ROLLBACK}
	return tx.proc.Rollback(expr)
}
