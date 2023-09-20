package db

import (
	"context"
)

type partitionIDKey struct{}

func ContextWithPartitionID(ctx context.Context, partitionID string) context.Context {
	if partitionID == "" {
		return ctx
	}
	return context.WithValue(ctx, partitionIDKey{}, partitionID)
}

func PartitionIDFromContext(ctx context.Context) string {
	// Don't panic
	id, _ := ctx.Value(partitionIDKey{}).(string)
	return id
}
