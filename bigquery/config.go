package bigquery

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/zeebo/errs/v2"
	"google.golang.org/api/option"

	"storj.io/eventkit"
	"storj.io/eventkit/destination"
)

// CreateDestination creates eventkit destination based on complex configuration.
// Example configurations:
//
//	127.0.0.1:1234
//	bigquery:app=...,project=...,dataset=...
//	bigquery:app=...,project=...,dataset=...|batch:queueSize=111,flashSize=111,flushInterval=111
//	bigquery:app=...,project=...,dataset=...|parallel:runners=10|batch:queueSize=111,flashSize=111,flushInterval=111
//	bigquery:app=...,project=...,dataset=...,credentialsPath=/path/to/my/service-account.json|parallel:runners=10|batch:queueSize=111
func CreateDestination(ctx context.Context, config string) (eventkit.Destination, error) {
	layers := strings.Split(config, "|")
	var lastLayer func() (eventkit.Destination, error)
	for _, layer := range layers {
		layer = strings.TrimSpace(layer)
		if layer == "" {
			continue
		}
		typeName, params, found := strings.Cut(layer, ":")
		if !found {
			return nil, errs.Errorf("eventkit destination parameters should be defined in the form type:param=value,...")
		}
		switch typeName {
		case "bigquery", "bq":
			var appName, project, dataset, credentialsPath string
			for _, param := range strings.Split(params, ",") {
				key, value, found := strings.Cut(param, "=")
				if !found {
					return nil, errs.Errorf("eventkit destination parameters should be defined in param2=value2 format")
				}
				switch key {
				case "appName":
					appName = value
				case "project":
					project = value
				case "dataset":
					dataset = value
				case "credentialsPath":
					credentialsPath = value
				default:
					return nil, errs.Errorf("Unknown parameter for bigquery destination %s. Please use appName/project/dataset", key)
				}

			}
			lastLayer = func() (eventkit.Destination, error) {
				var options []option.ClientOption
				if credentialsPath != "" {
					options = append(options, option.WithCredentialsFile(credentialsPath))
				}
				return NewBigQueryDestination(ctx, appName, project, dataset, options...)
			}
		case "parallel":
			var workers int
			for _, param := range strings.Split(params, ",") {
				key, value, found := strings.Cut(param, "=")
				if !found {
					return nil, errs.Errorf("eventkit destination parameters should be defined in param2=value2 format")
				}
				switch key {
				case "workers":
					var err error
					workers, err = strconv.Atoi(value)
					if err != nil {
						return nil, errs.Errorf("workers parameter of parallel destination should be a number and not %s", value)
					}
				default:
					return nil, errs.Errorf("Unknown parameter for parallel destination %s. Please use appName/project/dataset", value)
				}
			}

			ll := lastLayer
			lastLayer = func() (eventkit.Destination, error) {
				return destination.NewParallel(ll, workers), nil
			}

		case "batch":
			var queueSize, batchSize int
			var flushInterval time.Duration
			var err error
			for _, param := range strings.Split(params, ",") {
				key, value, found := strings.Cut(param, "=")
				if !found {
					return nil, errs.Errorf("eventkit destination parameters should be defined in param2=value2 format")
				}
				switch key {
				case "queueSize":
					queueSize, err = strconv.Atoi(value)
					if err != nil {
						return nil, errs.Errorf("queueSize parameter of batch destination should be a number and not %s", value)
					}
				case "batchSize":
					batchSize, err = strconv.Atoi(value)
					if err != nil {
						return nil, errs.Errorf("batchSize parameter of batch destination should be a number and not %s", value)
					}
				case "flushInterval":
					flushInterval, err = time.ParseDuration(value)
					if err != nil {
						return nil, errs.Errorf("flushInterval parameter of batch destination should be a duration and not %s", value)
					}
				default:
					return nil, errs.Errorf("Unknown parameter for batch destination %s. Please use queueSize/batchSize/flushInterval", key)
				}
			}
			ekDest, err := lastLayer()
			if err != nil {
				return nil, err
			}
			lastLayer = func() (eventkit.Destination, error) {
				return destination.NewBatchQueue(ekDest, queueSize, batchSize, flushInterval), nil
			}
		}
	}
	if lastLayer == nil {
		return nil, errs.Errorf("No evenkit destinatino is defined")
	}
	return lastLayer()
}
