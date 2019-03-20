package csvq

import (
	"database/sql/driver"
	"errors"
	"io"

	"github.com/mithrandie/csvq/lib/query"
	"github.com/mithrandie/csvq/lib/value"
	"github.com/mithrandie/ternary"
)

type ResultSet struct {
	view     *query.View
	rowIndex int
}

func NewResultSet(view *query.View) *ResultSet {
	return &ResultSet{
		view:     view,
		rowIndex: 0,
	}
}

func (r *ResultSet) Columns() []string {
	if r == nil {
		return nil
	}
	return r.view.Header.TableColumnNames()
}

func (r *ResultSet) Next(dest []driver.Value) error {
	if r == nil || r.view.RecordLen() <= r.rowIndex {
		return io.EOF
	}

	if len(r.view.RecordSet[r.rowIndex]) != len(dest) {
		return errors.New("column length does not match")
	}

	for i, v := range r.view.RecordSet[r.rowIndex] {
		val := v.Value()
		switch val.(type) {
		case value.String:
			dest[i] = val.(value.String).Raw()
		case value.Integer:
			dest[i] = val.(value.Integer).Raw()
		case value.Float:
			dest[i] = val.(value.Float).Raw()
		case value.Boolean:
			dest[i] = val.(value.Boolean).Raw()
		case value.Ternary:
			if val.Ternary() == ternary.UNKNOWN {
				dest[i] = nil
			} else {
				dest[i] = val.Ternary().ParseBool()
			}
		case value.Datetime:
			dest[i] = val.(value.Datetime).Raw()
		default: // Null
			dest[i] = nil
		}
	}

	r.rowIndex++
	return nil
}

type Rows struct {
	resultSets []*ResultSet
	index      int
}

func NewRows(selectedViews []*query.View) *Rows {
	sets := make([]*ResultSet, 0, len(selectedViews))
	for i := range selectedViews {
		sets = append(sets, NewResultSet(selectedViews[i]))
	}

	return &Rows{
		resultSets: sets,
		index:      0,
	}
}

func (r *Rows) Columns() []string {
	if len(r.resultSets) <= r.index {
		return nil
	}

	return r.resultSets[r.index].Columns()
}

func (r *Rows) Close() error {
	r.resultSets = nil
	r.index = 0
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	if len(r.resultSets) <= r.index || r.resultSets[r.index].view.RecordLen() <= r.index {
		return io.EOF
	}

	if len(r.resultSets[r.index].view.RecordSet[r.index]) != len(dest) {
		return errors.New("column length does not match")
	}

	return r.resultSets[r.index].Next(dest)
}

func (r *Rows) HasNextResultSet() bool {
	return r.index < len(r.resultSets)
}

func (r *Rows) NextResultSet() error {
	if !r.HasNextResultSet() {
		return io.EOF
	}
	r.index++
	return nil
}
