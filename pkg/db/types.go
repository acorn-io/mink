package db

import (
	"context"
	"time"

	"gorm.io/datatypes"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
)

type Record struct {
	ID         uint
	Kind       string
	Version    string
	APIGroup   string
	Name       string
	Namespace  string
	UID        string
	Generation int
	Previous   *uint
	Create     bool
	Created    time.Time
	Updated    time.Time
	Deleted    *time.Time
	Removed    *time.Time
	Garbage    *time.Time
	Metadata   datatypes.JSON
	Data       datatypes.JSON
	Status     datatypes.JSON
}

type WatchCriteria struct {
	Name          string
	Namespace     *string
	After         uint
	LabelSelector labels.Selector
}

type Criteria struct {
	Name      string
	Namespace *string
	// After is non-inclusive
	After uint
	// Before is inclusive
	Before            uint
	NoResourceVersion bool
	Limit             int64
	LabelSelector     labels.Selector
	FieldSelector     fields.Selector
	IncludeDeleted    bool
	IncludeGC         bool
}

type DB interface {
	Watch(ctx context.Context, criteria WatchCriteria) (chan Record, error)
	Get(ctx context.Context, criteria Criteria) ([]Record, uint, error)
	Insert(ctx context.Context, rec *Record) error
	Start(ctx context.Context)
}
