package db

import (
	"reflect"
	"strings"

	"github.com/acorn-io/mink/pkg/stores"
	"github.com/acorn-io/mink/pkg/strategy"
	"github.com/acorn-io/mink/pkg/types"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

type Factory struct {
	db     gorm.Dialector
	schema *runtime.Scheme
	stores map[string]rest.Storage
	err    error
}

func NewFactory(schema *runtime.Scheme, dsn string, stores map[string]rest.Storage) *Factory {
	var db gorm.Dialector
	if strings.HasPrefix(dsn, "postgres://") {
		db = postgres.Open(dsn)
	} else if dsn != "" {
		db = sqlite.Open(dsn)
	}
	if stores == nil {
		stores = map[string]rest.Storage{}
	}
	return &Factory{
		schema: schema,
		db:     db,
		stores: stores,
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
		if err := gdb.Table(tableName).AutoMigrate(&Record{}); err != nil {
			return nil, err
		}
	}
	s, err := NewStore(f.schema, obj, tableName, gdb)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (f *Factory) Err() error {
	return f.err
}

func (f *Factory) AddDBBased(resource string, obj kclient.Object, newStrategy strategy.AugmentComplete) {
	s, err := f.NewDBStrategy(obj)
	if err != nil {
		f.err = err
		return
	}
	if newStrategy != nil {
		s, err = newStrategy(s)
		if err != nil {
			f.err = err
			return
		}
	}
	store, status := stores.NewWithStatus(f.Scheme(), s)
	f.stores[resource] = store
	_, ok := reflect.TypeOf(obj).Elem().FieldByName("Status")
	if ok {
		f.stores[resource+"/status"] = status
	}
}
