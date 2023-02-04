package db

import (
	"fmt"
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

type TableNamer interface {
	TableName() string
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
		level := logger.Warn
		if logrus.IsLevelEnabled(logrus.TraceLevel) {
			level = logger.Info
		}
		gdb, err = gorm.Open(f.db, &gorm.Config{
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
		tableName = strings.ToLower(gvk.Kind)
		if tn, ok := obj.(TableNamer); ok {
			tableName = tn.TableName()
		}
		if f.AutoMigrate {
			gdb.Table(tableName).Model(&Record{}).Where("garbage IS NULL").Update("garbage", 0)
			if err := deleteCorrupted(gdb, tableName); err != nil {
				logrus.Error(err)
			}
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

func deleteCorrupted(db *gorm.DB, tableName string) error {
	for {
		if done, err := deleteCorruptedStep(db, tableName); err != nil {
			return err
		} else if done {
			return nil
		}
	}
}

func deleteCorruptedStep(db *gorm.DB, tableName string) (bool, error) {
	var result []Record

	resp := db.
		Table(tableName).
		Model(&Record{}).
		Select("max(id) as id, previous, count(previous) as p").
		Group("previous").
		Having("p > 1").
		Scan(&result)
	if resp.Error != nil {
		return false, fmt.Errorf("failed to cleanup corrupted data [%s]: %w", tableName, resp.Error)
	}

	if len(result) == 0 {
		return true, nil
	}

	badName := map[string]bool{}
	for _, record := range result {
		var badRecord Record
		resp := db.Table(tableName).
			Select("namespace", "name", "id", "previous").
			Model(&Record{}).
			First(&badRecord, record.ID)
		if resp.Error != nil {
			return false, fmt.Errorf("failed to cleanup corrupted data [%s]: %w", tableName, resp.Error)
		}

		logrus.Warnf("Duplicate previous %d on [%s/%s] %s %d", *badRecord.Previous, badRecord.Namespace,
			badRecord.Name, badRecord.Kind, record.ID)

		badKey := badRecord.Namespace + "/" + badRecord.Name
		if !badName[badKey] {
			logrus.Warnf("Marking %s %s/%s as garbage", tableName, badRecord.Namespace, badRecord.Name)
			resp = db.
				Table(tableName).Model(&Record{}).
				Where("namespace = ? and name = ?", badRecord.Namespace, badRecord.Name).
				Updates(map[string]any{
					"removed": time.Now(),
					"deleted": time.Now(),
				})
			if resp.Error != nil {
				return false, fmt.Errorf("failed to cleanup corrupted data [%s]: %w", tableName, resp.Error)
			}
			logrus.Warnf("Marked [%d] %s %s/%s as garbage", resp.RowsAffected, tableName, badRecord.Namespace, badRecord.Name)
		}

		badName[badKey] = true

		resp = db.
			Table(tableName).Model(&Record{}).
			Delete(badRecord, badRecord.ID)
		if resp.Error != nil {
			return false, fmt.Errorf("failed to cleanup corrupted data [%s]: %w", tableName, resp.Error)
		}
		logrus.Warnf("Deleted %s %s/%s [%d]", tableName, badRecord.Namespace, badRecord.Name, badRecord.ID)
	}

	return false, nil
}
