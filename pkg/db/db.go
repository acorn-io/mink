package db

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/acorn-io/mink/pkg/channel"
	"github.com/acorn-io/mink/pkg/datatypes"
	"gorm.io/gorm"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/klog/v2"
)

type GormDB struct {
	db          *gorm.DB
	tableName   string
	gvk         schema.GroupVersionKind
	trigger     chan struct{}
	broadcaster *channel.Broadcaster[Record]
}

func NewDB(tableName string, gvk schema.GroupVersionKind, db *gorm.DB) *GormDB {
	return &GormDB{
		gvk:         gvk,
		db:          db,
		tableName:   tableName,
		trigger:     make(chan struct{}, 1),
		broadcaster: channel.NewBroadcaster(make(chan Record)),
	}
}

func (g *GormDB) triggerWatchLoop() {
	select {
	case g.trigger <- struct{}{}:
	default:
	}
}

func (g *GormDB) sendBookmark(ctx context.Context, lastID uint) {
	if lastID == 0 {
		return
	}
	g.broadcaster.C <- Record{
		ID: lastID,
	}
}

func (g *GormDB) readEvents(ctx context.Context, lastID uint) (uint, error) {
	if lastID == 0 {
		id, err := g.getMaxID(ctx)
		if err != nil {
			return 0, err
		}
		lastID = id
	}

	records, err := g.since(ctx, lastID)
	if err != nil {
		return 0, err
	}

	// After this point never return an error because we need to record the
	// last ID sent in memory

	for _, record := range records {
		if record.ID != lastID+1 {
			g.fill(ctx, lastID)
			return lastID, nil
		}
		g.broadcaster.C <- record
		lastID = record.ID
	}

	return lastID, nil
}

func (g *GormDB) Start(ctx context.Context) {
	if g.db != nil {
		go g.broadcaster.Start(ctx)
		go g.watchLoop(ctx)
	}
}

func (g *GormDB) watchLoop(ctx context.Context) {
	var lastID uint

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		case <-time.After(time.Minute):
			g.sendBookmark(ctx, lastID)
			continue
		case <-g.trigger:
		}
		id, err := g.readEvents(ctx, lastID)
		if err != nil {
			klog.Infof("failed to initialize watcher: %v", err)
			continue
		}
		lastID = id
	}
}

func (g *GormDB) fill(ctx context.Context, id uint) {
	err := g.Insert(ctx, &Record{
		ID: id,
	})
	klog.Infof("failed to insert fill record for ID %d: %v", id, err)
}

func (g *GormDB) since(ctx context.Context, id uint) ([]Record, error) {
	var records []Record
	db := g.db.WithContext(ctx)
	resp := db.Table(g.tableName).Model(records).Where("id > ?", id).Find(&records)
	return records, resp.Error
}

func (g *GormDB) initializeWatch(ctx context.Context, criteria WatchCriteria, result chan<- Record) error {
	var (
		before = uint(0)
		after  = criteria.After
	)

	if after == 0 {
		return nil
	}

	for {
		resp, newBefore, err := g.Get(ctx, Criteria{
			Name:          criteria.Name,
			Namespace:     criteria.Namespace,
			After:         after,
			LabelSelector: criteria.LabelSelector,
			Limit:         1000,
			Before:        before,
		})
		if err != nil {
			return err
		}
		for _, record := range resp {
			result <- record
		}
		if len(resp) == 1000 {
			before = newBefore
			after = resp[len(resp)-1].ID
		} else {
			break
		}
	}

	return nil
}

func (g *GormDB) Watch(ctx context.Context, criteria WatchCriteria) (chan Record, error) {
	var (
		lastID     uint
		sub        = g.broadcaster.Subscribe()
		result     = make(chan Record)
		initialize = make(chan Record)
		merged     = channel.Concat(initialize, sub.C)
	)

	go func() {
		defer close(result)
		defer sub.Close()

		for {
			select {
			case <-ctx.Done():
				go func() {
					// ensure we empty this channel
					for range merged {
					}
				}()
				return
			case rec := <-merged:
				if lastID != 0 && rec.ID <= lastID {
					continue
				}
				lastID = rec.ID
				result <- rec
			}
		}
	}()

	err := g.initializeWatch(ctx, criteria, initialize)
	close(initialize)
	if err != nil {
		sub.Close()
		return nil, err
	}

	return result, nil
}

func (g *GormDB) getMaxID(ctx context.Context) (uint, error) {
	var (
		records []Record
		max     = Record{}
	)
	db := g.db.WithContext(ctx)
	result := db.Table(g.tableName).Model(records).Select("max(id) AS id").
		Order("id DESC").
		Limit(1).
		Scan(&max)
	return max.ID, result.Error
}

func (g *GormDB) find(ctx context.Context, db *gorm.DB, criteria Criteria) (result []Record, resourceVersion uint, err error) {
	db, resourceVersion, err = g.finalize(ctx, db, criteria)
	if err != nil {
		return nil, 0, err
	}
	db = db.Find(&result)
	return result, resourceVersion, db.Error
}

func (g *GormDB) finalize(ctx context.Context, db *gorm.DB, criteria Criteria) (*gorm.DB, uint, error) {
	joinQuery, resourceVersion, err := g.possibleIDs(ctx, criteria)
	if err != nil {
		return nil, 0, err
	}
	return db.Joins(
		fmt.Sprintf("join (?) j on j.id = %s.id", g.quote(g.tableName)), joinQuery), resourceVersion, nil
}

func (g *GormDB) possibleIDs(ctx context.Context, criteria Criteria) (*gorm.DB, uint, error) {
	query := g.newQuery(ctx).Select("namespace", "name", "max(id) AS id").
		Group("namespace").Group("name").
		Order("id ASC")

	if criteria.Namespace != nil {
		query.Where("namespace = ?", *criteria.Namespace)
	}
	if criteria.Name != "" {
		query.Where("name = ?", criteria.Name)
	}
	if criteria.After != 0 {
		query.Where("id > ?", criteria.After)
	}
	if criteria.NoResourceVersion {
		criteria.Before = 0
	} else if criteria.Before == 0 {
		max := Record{}
		result := g.newQuery(ctx).Select("max(id) AS id").
			Order("id DESC").
			Limit(1).
			Scan(&max)
		if result.Error != nil {
			return nil, 0, result.Error
		}
		criteria.Before = max.ID
	}
	if criteria.Before > 0 {
		query.Where("id <= ?", criteria.Before)
	}
	if !criteria.IncludeGC {
		query.Where("garbage IS NULL")
	}

	return query, criteria.Before, nil
}

func (g *GormDB) newQuery(ctx context.Context) *gorm.DB {
	var records []Record
	return g.db.WithContext(ctx).Table(g.tableName).Model(records)
}

func (g *GormDB) Get(ctx context.Context, criteria Criteria) ([]Record, uint, error) {
	query := g.newQuery(ctx)

	if criteria.Limit != 0 {
		query.Limit(int(criteria.Limit))
	}

	if !criteria.IncludeDeleted {
		query.Where("removed is NULL")
	}

	if criteria.LabelSelector != nil {
		reqs, ok := criteria.LabelSelector.Requirements()
		if ok {
			for _, req := range reqs {
				if req.Operator() == selection.Equals && req.Key() != "" {
					query.Where("? in ?", datatypes.JSONQuery("metadata").Value("labels", req.Key()), req.Values().List())
				}
				if req.Operator() == selection.In && req.Key() != "" {
					query.Where("? in ?", datatypes.JSONQuery("metadata").Value("labels", req.Key()), req.Values().List())
				}
				if req.Operator() == selection.NotIn && req.Key() != "" {
					query.Where("? not in ?", datatypes.JSONQuery("metadata").Value("labels", req.Key()), req.Values().List())
				}
				if req.Operator() == selection.NotEquals && req.Key() != "" {
					query.Where("? not in ?", datatypes.JSONQuery("metadata").Value("labels", req.Key()), req.Values().List())
				}
				if req.Operator() == selection.Exists && req.Key() != "" {
					query.Where(datatypes.JSONQuery("metadata").HasKey("labels", req.Key()))
				}
				if req.Operator() == selection.DoesNotExist && req.Key() != "" {
					query.Where(query.Not(datatypes.JSONQuery("metadata").HasKey("labels", req.Key())))
				}
			}
		} else {
			query.Where(false)
		}
	}

	if criteria.FieldSelector != nil {
		for _, req := range criteria.FieldSelector.Requirements() {
			if req.Field == "metadata.name" || req.Field == "metadata.namespace" {
				continue
			}
			if req.Operator == selection.Equals && req.Field != "" {
				parts := strings.Split(req.Field, ".")
				if parts[0] == "metadata" {
					continue
				}
				query.Where("? = ?", datatypes.JSONQuery("data").Value(parts...), req.Value)
			}
		}
	}

	return g.find(ctx, query, criteria)
}

func (g *GormDB) quote(s string) string {
	buf := &bytes.Buffer{}
	g.db.Dialector.QuoteTo(buf, s)
	return buf.String()
}

func (g *GormDB) Insert(ctx context.Context, rec *Record) error {
	defer g.triggerWatchLoop()
	return g.db.WithContext(ctx).Table(g.tableName).Create(rec).Error
}
