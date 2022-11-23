package db

import (
	"strings"

	"github.com/acorn-io/mink/pkg/strategy"
	"github.com/acorn-io/mink/pkg/types"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

type Factory struct {
	db          gorm.Dialector
	schema      *runtime.Scheme
	AutoMigrate bool
}

func NewFactory(schema *runtime.Scheme, dsn string) *Factory {
	dsn = strings.TrimPrefix(dsn, "mysql://")
	db := mysql.Open(dsn)
	return &Factory{
		AutoMigrate: true,
		schema:      schema,
		db:          db,
	}
}

func (f *Factory) Scheme() *runtime.Scheme {
	return f.schema
}

func (f *Factory) NewDBStrategy(obj types.Object) (strategy.CompleteStrategy, error) {
	gvk, err := apiutil.GVKForObject(obj, f.schema)
	if err != nil {
		return nil, err
	}

	var (
		gdb       *gorm.DB
		tableName string
	)
	if f.db != nil {
		gdb, err = gorm.Open(f.db, &gorm.Config{
			SkipDefaultTransaction: true,
		})
		if err != nil {
			return nil, err
		}
		tableName = strings.ToLower(gvk.Kind)
		if f.AutoMigrate {
			if err := gdb.Table(tableName).AutoMigrate(&Record{}); err != nil {
				return nil, err
			}
		}
	}
	s, err := NewStrategy(f.schema, obj, tableName, gdb)
	if err != nil {
		return nil, err
	}
	return s, nil
}
