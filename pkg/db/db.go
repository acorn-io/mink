package db

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/acorn-io/mink/pkg/channel"
	"github.com/acorn-io/mink/pkg/datatypes"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
)

const (
	defaultDeleteRetainCount     = 1000
	defaultCompactionRetainCount = 1000
	deleteBatchSize              = 1000
	compactBatchSize             = 1000
	watchLoopSleep               = 2 * time.Second
	defaultGCIntervalSeconds     = 1800
)

type GormDB struct {
	db          *gorm.DB
	tableName   string
	gvk         schema.GroupVersionKind
	trigger     chan struct{}
	broadcaster *channel.Broadcaster[Record]

	compactionLock sync.RWMutex
	compaction     uint
	lastIDLock     sync.Mutex
	lastID         uint
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
	g.broadcaster.C <- Record{
		ID: lastID,
	}
}

func (g *GormDB) readEvents(ctx context.Context, init bool, lastID uint) (uint, error) {
	if init {
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
			g.fill(ctx, lastID+1)
			return lastID, nil
		}
		if record.Name == "" && record.Namespace != "" {
			compact, err := strconv.Atoi(record.Namespace)
			if err == nil {
				g.compactionLock.Lock()
				if uint(compact) > g.compaction {
					g.compaction = uint(compact)
				}
				g.compactionLock.Unlock()
			}
		}
		g.broadcaster.C <- record
		lastID = record.ID
	}

	return lastID, nil
}

func (g *GormDB) Start(ctx context.Context) (err error) {
	// assume everything is compacted upfront
	g.compaction, err = g.getMaxID(ctx)
	if err != nil {
		return err
	}
	if g.db != nil {
		go g.broadcaster.Start(ctx)
		go g.watchLoop(ctx)
		go g.gc(ctx)
	}
	return nil
}

func (g *GormDB) getEnv(key string, def uint) uint {
	envNames := []string{
		key + "_" + strings.ToUpper(g.tableName),
		key + "_" + g.tableName,
		key,
	}
	for _, envName := range envNames {
		s := os.Getenv(envName)
		if s != "" {
			i, err := strconv.Atoi(s)
			if err != nil {
				panic(fmt.Sprintf("Invalid value %s=%s: %v", envName, s, err))
			}
			return uint(i)
		}
	}

	return def
}

func (g *GormDB) getCompactRetainCount() uint {
	return g.getEnv("MINK_COMPACT_RETAIN", defaultCompactionRetainCount)
}

func (g *GormDB) getDeleteRetainCount() int {
	return int(g.getEnv("MINK_DELETE_RETAIN", defaultDeleteRetainCount))
}

func (g *GormDB) gc(ctx context.Context) {
	if g.getCompactRetainCount() == 0 {
		logrus.Debugf("Compaction and deletion disabled for [%s]", g.tableName)
		return
	}

	var (
		lastSuccessCompaction uint
		// first loop is less delay
		delay = wait.Jitter(10*time.Second, 2)
	)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}

		delay = wait.Jitter(time.Duration(g.getEnv("MINK_GC_INTERVAL_SECONDS", defaultGCIntervalSeconds))*time.Second, 0)

		if lastSuccessCompaction == 0 {
			logrus.Debugf("Starting compaction goroutine for [%s]", g.tableName)
			minID, err := g.getMinID(ctx)
			if err != nil {
				logrus.Errorf("failed to get minimum ID for compaction: %v", err)
			}
			lastSuccessCompaction = minID
		}

		g.lastIDLock.Lock()
		nextCompactionID := g.lastID
		g.lastIDLock.Unlock()

		if nextCompactionID < g.getCompactRetainCount() {
			continue
		}
		nextCompactionID -= g.getCompactRetainCount()

		if cont, err := g.markCompaction(ctx, nextCompactionID); err != nil {
			g.compactionLock.Unlock()
			logrus.Errorf("Failed to write compaction record [%s] %d: %v", g.tableName, nextCompactionID, err)
			continue
		} else if !cont {
			logrus.Debugf("Skipping compaction [%s]", g.tableName)
			continue
		}

		g.compactionLock.Lock()
		if nextCompactionID > g.compaction {
			g.compaction = nextCompactionID
		}
		g.compactionLock.Unlock()

		// Make sure peers know about compaction change
		time.Sleep(2 * watchLoopSleep)

		for lastSuccessCompaction < nextCompactionID {
			var (
				records []Record
				ids     []uint
			)

			nextBatch := lastSuccessCompaction + compactBatchSize
			if nextBatch > nextCompactionID {
				nextBatch = nextCompactionID
			}

			logrus.Debugf("Running compaction [%s] %d => %d", g.tableName, lastSuccessCompaction, nextBatch)
			db := g.newQuery(ctx).
				Select("id", "name", "removed", "previous").
				Where("id >= ? and id < ?", lastSuccessCompaction, nextBatch).Scan(&records)
			if db.Error != nil {
				logrus.Errorf("Failed running compaction [%s] %d => %d: %v", g.tableName, lastSuccessCompaction, nextBatch,
					db.Error)
				continue
			}

			for _, record := range records {
				if record.Previous != nil {
					ids = append(ids, *record.Previous)
				}
				// delete fill records or removed
				if record.Name == "" || record.Removed != nil {
					ids = append(ids, record.ID)
				}
			}

			db = g.newQuery(ctx).
				Where("garbage is FALSE and id in (?)", ids).
				Update("garbage", true)
			if db.Error != nil {
				logrus.Errorf("Failed updating compaction [%s] %d => %d: %v", g.tableName, lastSuccessCompaction, nextBatch,
					db.Error)
			} else if db.RowsAffected > 0 {
				logrus.Debugf("compacted [%s] [%d] rows", g.tableName, db.RowsAffected)
			}

			lastSuccessCompaction = nextBatch
		}

		deleteCount := g.getDeleteRetainCount()
		if deleteCount == 0 {
			logrus.Debugf("Deletion disabled for [%s]", g.tableName)
			continue
		}

		for {
			var (
				ids []uint
			)

			db := g.newQuery(ctx).
				Select("id").
				Where("garbage IS TRUE").
				Order("id ASC").
				Limit(deleteCount + deleteBatchSize).
				Scan(&ids)
			if db.Error != nil {
				logrus.Errorf("Failed finding deletion [%s]: %v", g.tableName, db.Error)
				continue
			}

			if len(ids) > deleteCount {
				ids = ids[:len(ids)-deleteCount]
				logrus.Debugf("Deleting [%d] records for [%s]: %v", len(ids), g.tableName, ids)
				db := g.newQuery(ctx).
					Delete("id in ?", ids)
				if db.Error != nil {
					logrus.Errorf("Failed running deletion [%s]: %v", g.tableName, db.Error)
				}
			} else {
				break
			}
		}
	}
}
func (g *GormDB) watchLoop(ctx context.Context) {
	var (
		lastID uint
		init   = true
	)

	for {
		// set last id for compaction
		g.lastIDLock.Lock()
		g.lastID = lastID
		g.lastIDLock.Unlock()

		select {
		case <-ctx.Done():
			return
		case <-time.After(watchLoopSleep):
		case <-time.After(time.Minute):
			g.sendBookmark(ctx, lastID)
			continue
		case <-g.trigger:
		}
		id, err := g.readEvents(ctx, init, lastID)
		if err != nil {
			klog.Infof("failed to initialize watcher: %v", err)
			continue
		}
		init = false
		lastID = id
	}
}

func (g *GormDB) markCompaction(ctx context.Context, id uint) (bool, error) {
	cont := false
	err := g.Transaction(ctx, func(ctx context.Context) error {
		var lastRecord Record
		resp := g.newQuery(ctx).Last(&lastRecord)
		if resp.Error != nil {
			return resp.Error
		}
		if lastRecord.Name == "" && lastRecord.Namespace != "" {
			// last record is compaction record, so don't insert another, otherwise we just
			// continue to insert compaction records as the only new data in the table
			return nil
		}
		cont = true
		logrus.Debugf("Inserting compaction record for [%s] [%d]", g.tableName, id)
		return g.Insert(ctx, &Record{
			Namespace: strconv.FormatUint(uint64(id), 10),
		})
	})
	return cont, err
}

func (g *GormDB) fill(ctx context.Context, id uint) {
	err := g.Insert(ctx, &Record{
		ID: id,
	})
	if err != nil {
		klog.Infof("failed to insert fill record for ID %d: %v", id, err)
	}
}

func (g *GormDB) since(ctx context.Context, id uint) ([]Record, error) {
	var records []Record
	db := g.getDB(ctx).WithContext(ctx)
	resp := db.Table(g.tableName).Model(records).Where("id > ?", id).Find(&records)
	return records, resp.Error
}

func (g *GormDB) initializeWatch(ctx context.Context, criteria WatchCriteria, result chan<- Record) error {
	var (
		before = uint(0)
		after  = criteria.After
	)

	for {
		resp, newBefore, err := g.Get(ctx, Criteria{
			Name:                  criteria.Name,
			Namespace:             criteria.Namespace,
			After:                 after,
			LabelSelector:         criteria.LabelSelector,
			Limit:                 1000,
			Before:                before,
			ignoreCompactionCheck: true,
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

// validateCriteria should be called while holding the g.compactionLock
func (g *GormDB) validateCriteria(before, after uint) error {
	if before != 0 && before < g.compaction {
		return newCompactionError(before, g.compaction)
	}
	if after != 0 && after < g.compaction {
		return newCompactionError(after, g.compaction)
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

	// this will be released after the initializeWatch is done
	g.compactionLock.RLock()
	if err := g.validateCriteria(0, criteria.After); err != nil {
		g.compactionLock.RUnlock()
		close(result)
		close(initialize)
		sub.Close()
		go func() {
			// ensure we empty this channel
			for range merged {
			}
		}()
		return nil, err
	}

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
			case rec, ok := <-merged:
				if !ok {
					// This means that both initialize and sub.C have been closed.
					return
				}
				if lastID != 0 && rec.ID <= lastID {
					continue
				}
				lastID = rec.ID
				result <- rec
			}
		}
	}()

	go func() {
		err := g.initializeWatch(ctx, criteria, initialize)
		g.compactionLock.RUnlock()
		close(initialize)
		if err != nil {
			logrus.Errorf("error initializing watch for kind %s: %v", g.gvk.Kind, err)
			sub.Close()
		}
	}()

	return result, nil
}

func (g *GormDB) getMinID(ctx context.Context) (uint, error) {
	var (
		records []Record
		max     = Record{}
	)
	db := g.getDB(ctx).WithContext(ctx)
	result := db.Table(g.tableName).Model(records).Select("id").
		Where("garbage IS FALSE").
		Order("id ASC").
		Limit(1).
		Scan(&max)
	return max.ID, result.Error
}

func (g *GormDB) getMaxID(ctx context.Context) (uint, error) {
	var (
		records []Record
		max     = Record{}
	)
	db := g.getDB(ctx).WithContext(ctx)
	result := db.Table(g.tableName).Model(records).Select("id").
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
	if !criteria.ignoreCompactionCheck {
		g.compactionLock.RLock()
		if err := g.validateCriteria(criteria.Before, criteria.After); err != nil {
			g.compactionLock.RUnlock()
			return nil, 0, err
		}
	}
	db = db.Find(&result)
	if !criteria.ignoreCompactionCheck {
		g.compactionLock.RUnlock()
	}
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
	} else {
		// don't pick up fill or compaction records
		query.Where("name != ?", "")
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
		query.Where("garbage IS FALSE")
	}

	return query, criteria.Before, nil
}

func (g *GormDB) newQuery(ctx context.Context) *gorm.DB {
	var records []Record
	return g.getDB(ctx).WithContext(ctx).Table(g.tableName).Model(records)
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
				l := datatypes.JSONQuery("metadata").Value("labels", req.Key())
				if req.Operator() == selection.Equals && req.Key() != "" && req.Values().Len() == 1 {
					query.Where("? = ?", l, req.Values().List()[0])
				}
				if req.Operator() == selection.In && req.Key() != "" {
					query.Where("? in ?", l, req.Values().List())
				}
				if req.Operator() == selection.NotIn && req.Key() != "" {
					query.Where("? not in ?", l, req.Values().List())
				}
				if req.Operator() == selection.NotEquals && req.Key() != "" && req.Values().Len() == 1 {
					query.Where("? = ?", l, req.Values().List()[0])
				}
				if req.Operator() == selection.Exists && req.Key() != "" {
					query.Where(l.Exists())
				}
				if req.Operator() == selection.DoesNotExist && req.Key() != "" {
					query.Where("not ?", l.Exists())
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

type dbKey struct{}

func (g *GormDB) getDB(ctx context.Context) *gorm.DB {
	db, ok := ctx.Value(dbKey{}).(*gorm.DB)
	if ok {
		return db
	}
	return g.db
}

func (g *GormDB) Transaction(ctx context.Context, do func(ctx context.Context) error) error {
	return g.getDB(ctx).Transaction(func(tx *gorm.DB) error {
		return do(context.WithValue(ctx, dbKey{}, tx))
	})
}

func (g *GormDB) Insert(ctx context.Context, rec *Record) error {
	defer g.triggerWatchLoop()
	return g.getDB(ctx).Transaction(func(tx *gorm.DB) error {
		tx = tx.WithContext(ctx)
		if rec.Previous != nil {
			db := tx.Table(g.tableName).Where("id = ?", *rec.Previous).
				Update("latest", false)
			if db.Error != nil {
				return db.Error
			}
		}
		if rec.Name != "" {
			rec.Latest = true
		}
		return tx.Table(g.tableName).Create(rec).Error
	})
}
