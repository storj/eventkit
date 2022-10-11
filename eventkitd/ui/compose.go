package ui

import (
	"crypto/rand"
	"net/http"

	"gopkg.in/webhelp.v1/whcache"
	"gopkg.in/webhelp.v1/whfatal"
	"gopkg.in/webhelp.v1/whlog"
	"gopkg.in/webhelp.v1/whmux"
	"gopkg.in/webhelp.v1/whsess"
)

var cookieSecret = func() []byte {
	var buf [32]byte
	_, err := rand.Read(buf[:])
	if err != nil {
		panic(err)
	}
	return buf[:]
}()

func Handler(dataPath string) http.Handler {
	return whcache.Register(
		whlog.LogRequests(whlog.Default, whlog.LogResponses(whlog.Default,
			whsess.HandlerWithStore(whsess.NewCookieStore(cookieSecret),
				whfatal.Catch(whmux.Dir{
					"":       mainHandler(),
					"render": renderHandler(dataPath),
				})))))
}

type graph struct{}
type target struct{}

func mainHandler() http.Handler {
	return whmux.Exact(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tmplMain.Execute(w, &struct {
			Graphs  []graph
			Targets []target
		}{})
	}))
}
