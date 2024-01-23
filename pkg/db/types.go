package db

import (
	"context"
	"time"

	"gorm.io/datatypes"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
)

type Record struct {
	ID          uint
	Kind        string
	Version     string
	APIGroup    string
	Name        string `gorm:"index:,composite:idx_ns_name_id"`
	Namespace   string `gorm:"index:,composite:idx_ns_name_id"`
	UID         string
	Generation  int
	Previous    *uint `gorm:"index:,unique"`
	Create      bool
	Created     time.Time
	Updated     time.Time
	Deleted     *time.Time
	Removed     *time.Time
	Garbage     bool `gorm:"index:,composite:idx_garbage;not null;default:0"`
	Latest      bool `gorm:"index:,composite:idx_latest;default:0"`
	Metadata    datatypes.JSON
	Data        datatypes.JSON
	Status      datatypes.JSON
	PartitionID string `gorm:"index:,composite:idx_ns_name_id"`
}

type WatchCriteria struct {
	Name          string
	Namespace     *string
	After         uint
	LabelSelector labels.Selector
	FieldSelector fields.Selector
	PartitionID   string
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
	PartitionID       string

	ignoreCompactionCheck bool
}

type DB interface {
	Transaction(ctx context.Context, do func(ctx context.Context) error) error
	Watch(ctx context.Context, criteria WatchCriteria) (chan Record, error)
	Get(ctx context.Context, criteria Criteria) ([]Record, uint, error)
	Insert(ctx context.Context, rec *Record) error
	Start(ctx context.Context) error
}
