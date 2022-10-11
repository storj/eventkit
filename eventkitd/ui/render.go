package ui

import (
	"fmt"
	"net/http"

	"gopkg.in/webhelp.v1/whfatal"
	"gopkg.in/webhelp.v1/whmux"

	"github.com/jtolio/eventkit/pb"
)

type DataSink interface {
	Add(name string, scope []string, data *pb.Record) error
}

var renderers = map[string]func(w http.ResponseWriter, r *http.Request) DataSink{
	"csv": func(w http.ResponseWriter, r *http.Request) DataSink {
		w.Header().Set("Content-Type", "text/csv")
	},
}

func renderHandler(dataPath string) http.Handler {
	return whmux.Exact(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rendererName := r.FormValue("renderer")
		if rendererName == "" {
			rendererName = "csv"
		}
		renderer, ok := renderers[rendererName]
		if !ok {
			whfatal.Error(fmt.Errorf("unknown renderer %q", rendererName))
		}
	}))
}
