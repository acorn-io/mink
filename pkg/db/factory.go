package db

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/acorn-io/mink/pkg/db/glogrus"
	"github.com/acorn-io/mink/pkg/strategy"
	"github.com/acorn-io/mink/pkg/types"
	"github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/server/options/encryptionconfig"
	"k8s.io/apiserver/pkg/storage/value"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

type Factory struct {
	db                  *gorm.DB
	sqlDB               *sql.DB
	schema              *runtime.Scheme
	migrationTimeout    time.Duration
	AutoMigrate         bool
	transformers        map[schema.GroupKind]value.Transformer
	partitionIDRequired bool
}

type FactoryOption func(*Factory)

// WithMigrationTimeout sets a timeout for the initial database migration if auto migration is enabled.
func WithMigrationTimeout(timeout time.Duration) FactoryOption {
	return func(f *Factory) {
		f.migrationTimeout = timeout
	}
}

func WithEncryptionConfiguration(ctx context.Context, configPath string) (FactoryOption, error) {
	encryptionConf, err := encryptionconfig.LoadEncryptionConfig(ctx, configPath, false)
	if err != nil {
		return nil, err
	}

	// Although the config reading code expects GroupResources, we expect GroupKinds,
	// so just convert, assuming that the Resource is actually a Kind, but lowercase.
	transformers := make(map[schema.GroupKind]value.Transformer, len(encryptionConf.Transformers))
	for gr, t := range encryptionConf.Transformers {
		if len(gr.Resource) < 2 {
			return nil, fmt.Errorf("invalid Resource: %s", gr.Resource)
		}

		transformers[schema.GroupKind{Group: gr.Group, Kind: strings.ToUpper(gr.Resource[:1]) + gr.Resource[1:]}] = t
	}

	return func(f *Factory) {
		f.transformers = transformers
	}, nil
}

// WithPartitionIDRequired will configure the all DB strategies created from this factory to require a partition ID when querying the database.
func WithPartitionIDRequired() FactoryOption {
	return func(f *Factory) {
		f.partitionIDRequired = true
	}
}

func NewFactory(schema *runtime.Scheme, dsn string, opts ...FactoryOption) (*Factory, error) {
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
		Logger: glogrus.New(glogrus.Config{
			SlowThreshold:             200 * time.Millisecond,
			IgnoreRecordNotFoundError: false,
			LogSQL:                    true,
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
	f.sqlDB = sqlDB
	return f, nil
}

func (f *Factory) Scheme() *runtime.Scheme {
	return f.schema
}

func (f *Factory) Name() string {
	return "Mink DB"
}

func (f *Factory) Check(req *http.Request) error {
	err := f.sqlDB.PingContext(req.Context())
	if err != nil {
		logrus.Warnf("Failed to ping database: %v", err)
	}

	return err
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
	s, err := NewStrategy(f.schema, obj, tableName, f.db, f.transformers, f.partitionIDRequired)
	if err != nil {
		return nil, err
	}
	return s, nil
}
