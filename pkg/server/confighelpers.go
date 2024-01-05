package server

import (
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/options"
	"k8s.io/apiserver/plugin/pkg/audit/buffered"
	"k8s.io/apiserver/plugin/pkg/audit/log"
)

type APIGroupFunc[T any] func(T) (*server.APIGroupInfo, error)

func BuildAPIGroups[T any](arg T, factories ...APIGroupFunc[T]) (result []*server.APIGroupInfo, err error) {
	for _, factory := range factories {
		apiGroup, err := factory(arg)
		if err != nil {
			return nil, err
		}
		result = append(result, apiGroup)
	}
	return result, nil
}

func NewAuditOptions(policyFile, logPath string) *options.AuditOptions {
	return &options.AuditOptions{
		PolicyFile: policyFile,
		LogOptions: options.AuditLogOptions{
			Path:    logPath,
			MaxSize: 250,
			Format:  log.FormatJson,
			BatchOptions: options.AuditBatchOptions{
				Mode: options.ModeBlocking,
				BatchConfig: buffered.BatchConfig{
					BufferSize: 10000,
					// Batching is not useful for the log-file backend.
					// MaxBatchWait ignored.
					MaxBatchSize:   1,
					ThrottleEnable: false,
					// Asynchronous log threads just create lock contention.
					AsyncDelegate: false,
				},
			},
			TruncateOptions:    options.NewAuditTruncateOptions(),
			GroupVersionString: "audit.k8s.io/v1",
		},
	}
}
