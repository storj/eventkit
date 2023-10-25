// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package listener

import (
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/spacemonkeygo/monkit/v3"
)

// ApplyNewTransformers adds the default set of monkit.CallbackTransformers.
// This needs to happen individually for each output type and endpoint.
func ApplyNewTransformers(r *monkit.Registry) *monkit.Registry {
	return r.WithTransformers(monkit.NewDeltaTransformer())
}

// PrometheusEndpoint includes all the information to server Prometheus compatible HTTP pages.
type PrometheusEndpoint struct {
	registryMu   sync.Mutex
	registries   map[string]*monkit.Registry
	baseRegistry *monkit.Registry
}

// NewPrometheusEndpoint creates an initialized PrometheusEndpoint.
func NewPrometheusEndpoint(registry *monkit.Registry) *PrometheusEndpoint {
	return &PrometheusEndpoint{
		baseRegistry: registry,
		registries:   map[string]*monkit.Registry{},
	}
}

// PrometheusMetrics writes monkit data in  https://prometheus.io/docs/instrumenting/exposition_formats/.
func (server *PrometheusEndpoint) PrometheusMetrics(w http.ResponseWriter, r *http.Request) {
	// We have to collect all of the metrics before we write. This is because we do not
	// get all of the metrics from the registry sorted by measurement, and from the docs:
	//
	//     All lines for a given metric must be provided as one single group, with the
	//     optional HELP and TYPE lines first (in no particular order). Beyond that,
	//     reproducible sorting in repeated expositions is preferred but not required,
	//     i.e. do not sort if the computational cost is prohibitive.

	data := make(map[string][]string)
	var components []string

	server.registryForRequest(r).Stats(func(key monkit.SeriesKey, field string, val float64) {
		components = components[:0]

		measurement := sanitize(key.Measurement)
		for tag, tagVal := range key.Tags.All() {
			components = append(components,
				fmt.Sprintf("%s=%q", sanitize(tag), sanitize(tagVal)))
		}
		components = append(components,
			fmt.Sprintf("field=%q", sanitize(field)))

		data[measurement] = append(data[measurement],
			fmt.Sprintf("{%s} %g", strings.Join(components, ","), val))
	})

	for measurement, samples := range data {
		_, _ = fmt.Fprintln(w, "# TYPE", measurement, "gauge")
		for _, sample := range samples {
			_, _ = fmt.Fprintf(w, "%s%s\n", measurement, sample)
		}
	}
}

func (server *PrometheusEndpoint) registryForRequest(r *http.Request) *monkit.Registry {
	outputID := r.URL.Query().Get("output-id")
	server.registryMu.Lock()
	defer server.registryMu.Unlock()
	// okay if outputID is ""
	reg, found := server.registries[outputID]
	if !found {
		reg = ApplyNewTransformers(server.baseRegistry)
		server.registries[outputID] = reg
	}
	return reg
}

// sanitize formats val to be suitable for prometheus.
func sanitize(val string) string {
	// https://prometheus.io/docs/concepts/data_model/
	// specifies all metric names must match [a-zA-Z_:][a-zA-Z0-9_:]*
	// Note: The colons are reserved for user defined recording rules.
	// They should not be used by exporters or direct instrumentation.
	if val == "" {
		return ""
	}
	if '0' <= val[0] && val[0] <= '9' {
		val = "_" + val
	}
	return strings.Map(func(r rune) rune {
		switch {
		case 'a' <= r && r <= 'z':
			return r
		case 'A' <= r && r <= 'Z':
			return r
		case '0' <= r && r <= '9':
			return r
		default:
			return '_'
		}
	}, val)
}
