package csvq

import (
	"io"
	"sync"

	"github.com/mithrandie/csvq/lib/query"
)

var session *query.Session
var getSessionOnce sync.Once

func getSession() *query.Session {
	getSessionOnce.Do(func() {
		session = query.NewSession()
		session.Stdout = &query.Discard{}
		session.Stderr = &query.Discard{}
	})
	return session
}

func SetStdin(r io.ReadCloser) {
	getSession().Stdin = r
}

func SetStdout(w io.WriteCloser) {
	getSession().Stdout = w
}

func SetOutFile(w io.Writer) {
	getSession().OutFile = w
}
