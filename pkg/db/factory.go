package db

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	"github.com/acorn-io/mink/pkg/strategy"
	"github.com/acorn-io/mink/pkg/types"
	"github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

type Factory struct {
	db               *gorm.DB
	schema           *runtime.Scheme
	migrationTimeout time.Duration
	AutoMigrate      bool
}

type FactoryOption func(*Factory)

// WithMigrationTimeout sets a timeout for the initial database migration if auto migration is enabled.
func WithMigrationTimeout(timeout time.Duration) FactoryOption {
	return func(f *Factory) {
		f.migrationTimeout = timeout
	}
}

func NewFactory(schema *runtime.Scheme, dsn string, opts ...FactoryOption) (*Factory, error) {
	level := logger.Warn
	if logrus.IsLevelEnabled(logrus.TraceLevel) {
		level = logger.Info
	}
	f := &Factory{
		AutoMigrate: true,
		schema:      schema,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(f)
		}
	}

	dsn = strings.TrimPrefix(dsn, "mysql://")
	gdb := mysql.Open(dsn)
	db, err := gorm.Open(gdb, &gorm.Config{
		SkipDefaultTransaction: true,
		Logger: logger.New(log.New(os.Stdout, "\r\n", log.LstdFlags), logger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  level,
			IgnoreRecordNotFoundError: false,
			Colorful:                  true,
		}),
	})
	if err != nil {
		return nil, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetConnMaxLifetime(time.Minute * 3)
	sqlDB.SetMaxIdleConns(5)
	sqlDB.SetMaxOpenConns(5)
	f.db = db
	return f, nil
}

func (f *Factory) Scheme() *runtime.Scheme {
	return f.schema
}

type TableNamer interface {
	TableName() string
}

func (f *Factory) NewDBStrategy(obj types.Object) (strategy.CompleteStrategy, error) {
	gvk, err := apiutil.GVKForObject(obj, f.schema)
	if err != nil {
		return nil, err
	}

	var (
		tableName string
	)
	if f.db != nil {
		tableName = strings.ToLower(gvk.Kind)
		if tn, ok := obj.(TableNamer); ok {
			tableName = tn.TableName()
		}
		if f.AutoMigrate {
			ctx := context.Background()
			if f.migrationTimeout != 0 {
				// If configured, set a timeout for the migration
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, f.migrationTimeout)
				defer cancel()
			}

			if err := f.db.WithContext(ctx).Table(tableName).AutoMigrate(&Record{}); err != nil {
				return nil, err
			}
		}
	}
	s, err := NewStrategy(f.schema, obj, tableName, f.db)
	if err != nil {
		return nil, err
	}
	return s, nil
}
