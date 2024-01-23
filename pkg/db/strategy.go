package db

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/acorn-io/mink/pkg/types"
	"gorm.io/gorm"
	apierror "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/value"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

type Strategy struct {
	scheme              *runtime.Scheme
	db                  DB
	obj                 runtime.Object
	objList             runtime.Object
	gvk                 schema.GroupVersionKind
	partitionIDRequired bool

	dbCtx    context.Context
	dbCancel func()
}

type cont struct {
	ID uint `json:"id,omitempty"`
}

func NewStrategy(scheme *runtime.Scheme, obj runtime.Object, tableName string, db *gorm.DB, transformers map[schema.GroupKind]value.Transformer, partitionIDRequired bool) (*Strategy, error) {
	gvk, err := apiutil.GVKForObject(obj, scheme)
	if err != nil {
		return nil, err
	}

	// test we can create objects
	_, err = scheme.New(gvk)
	if err != nil {
		return nil, err
	}

	objList, err := scheme.New(schema.GroupVersionKind{
		Group:   gvk.Group,
		Version: gvk.Version,
		Kind:    gvk.Kind + "List",
	})
	s := &Strategy{
		scheme:              scheme,
		db:                  NewDB(tableName, gvk, db, transformers),
		gvk:                 gvk,
		obj:                 obj,
		objList:             objList,
		partitionIDRequired: partitionIDRequired,
	}
	s.dbCtx, s.dbCancel = context.WithCancel(context.Background())
	return s, s.db.Start(s.dbCtx)
}

func (s *Strategy) Destroy() {
	s.dbCancel()
}

func (s *Strategy) Start(ctx context.Context) {
	s.db.Start(ctx)
}

func (s *Strategy) Get(ctx context.Context, namespace, name string) (types.Object, error) {
	partitionID := PartitionIDFromContext(ctx)
	if s.partitionIDRequired && partitionID == "" {
		return nil, newPartitionRequiredError()
	}

	records, _, err := s.db.Get(ctx, Criteria{
		Name:              name,
		Namespace:         strptr(namespace),
		NoResourceVersion: true,
		PartitionID:       partitionID,
	})
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, newNotFound(s.gvk, name)
	}

	obj := s.obj.DeepCopyObject()
	return obj.(types.Object), s.recordIntoObject(&records[0], obj)
}

func (s *Strategy) GetToList(ctx context.Context, namespace, name string) (types.ObjectList, error) {
	partitionID := PartitionIDFromContext(ctx)
	if s.partitionIDRequired && partitionID == "" {
		return nil, newPartitionRequiredError()
	}

	list := s.objList.DeepCopyObject().(types.ObjectList)
	obj := s.obj.DeepCopyObject()

	records, resourceVersionInt, err := s.db.Get(ctx, Criteria{
		Name:        name,
		Namespace:   strptr(namespace),
		PartitionID: partitionID,
	})
	if err != nil {
		return nil, err
	}

	list.SetResourceVersion(strconv.FormatUint(uint64(resourceVersionInt), 10))

	if len(records) == 0 {
		return list, nil
	}

	if err := s.recordIntoObject(&records[0], obj); err != nil {
		return nil, err
	}

	return list, meta.SetList(list, []runtime.Object{obj})
}

func (s *Strategy) Watch(ctx context.Context, namespace string, opts storage.ListOptions) (<-chan watch.Event, error) {
	partitionID := PartitionIDFromContext(ctx)
	if s.partitionIDRequired && partitionID == "" {
		return nil, newPartitionRequiredError()
	}

	criteria := WatchCriteria{
		Namespace:     nilOnEmpty(namespace),
		LabelSelector: opts.Predicate.Label,
		FieldSelector: opts.Predicate.Field,
		PartitionID:   partitionID,
	}
	name, ok := opts.Predicate.MatchesSingle()
	if ok {
		criteria.Name = name
	}
	if opts.ResourceVersion != "" {
		after, err := strconv.ParseUint(opts.ResourceVersion, 10, 64)
		if err != nil {
			return nil, err
		}
		criteria.After = uint(after)
	}
	records, err := s.db.Watch(ctx, criteria)
	if err != nil {
		return nil, err
	}

	result := make(chan watch.Event)
	go func() {
		defer close(result)

		for record := range records {
			obj := s.newObj()
			if record.Name == "" {
				obj.SetResourceVersion(strconv.FormatUint(uint64(record.ID), 10))
				if opts.Predicate.AllowWatchBookmarks {
					result <- watch.Event{
						Type:   watch.Bookmark,
						Object: obj,
					}
				}
				continue
			}

			if criteria.PartitionID != "" && record.PartitionID != criteria.PartitionID {
				continue
			}

			if criteria.Name != "" && record.Name != criteria.Name {
				continue
			}

			if criteria.Namespace != nil && record.Namespace != *criteria.Namespace {
				continue
			}

			event := watch.Event{}
			match := true
			err := s.recordIntoObject(&record, obj)
			if err == nil {
				match, err = opts.Predicate.Matches(obj)
			}
			if err != nil {
				event.Type = watch.Error
				status := apierror.NewGenericServerResponse(http.StatusInternalServerError, "watch", schema.GroupResource{
					Group:    s.gvk.Group,
					Resource: s.gvk.Kind,
				}, record.Name, err.Error(), 0, true).Status()
				event.Object = &status
				result <- event
			} else if match {
				if record.Create {
					event.Type = watch.Added
					event.Object = obj
				} else if record.Removed != nil {
					event.Type = watch.Deleted
					event.Object = obj
				} else {
					event.Type = watch.Modified
					event.Object = obj
				}
				result <- event
			}
		}
	}()

	return result, nil
}

func (s *Strategy) New() types.Object {
	return s.obj.DeepCopyObject().(types.Object)
}

func (s *Strategy) NewList() types.ObjectList {
	return s.objList.DeepCopyObject().(types.ObjectList)
}

func (s *Strategy) newObj() types.Object {
	obj, err := s.scheme.New(s.gvk)
	if err != nil {
		panic("failed to create object for watch: " + err.Error())
	}
	return obj.(types.Object)
}

func (s *Strategy) List(ctx context.Context, namespace string, opts storage.ListOptions) (types.ObjectList, error) {
	partitionID := PartitionIDFromContext(ctx)
	if s.partitionIDRequired && partitionID == "" {
		return nil, newPartitionRequiredError()
	}

	list := s.objList.DeepCopyObject().(types.ObjectList)

	result, err := s.list(ctx, nilOnEmpty(namespace), partitionID, opts)
	if err != nil {
		return nil, err
	}

	list.SetResourceVersion(result.ResourceVersion)
	list.SetContinue(result.Continue)
	list.SetRemainingItemCount(result.RemainingCount)
	return list, meta.SetList(list, result.Items)
}

type listResult struct {
	Items           []runtime.Object
	Continue        string
	ResourceVersion string
	RemainingCount  *int64
}

func (s *Strategy) list(ctx context.Context, namespace *string, partitionID string, opts storage.ListOptions) (*listResult, error) {
	result := &listResult{}

	if opts.Predicate.Limit != 0 {
		opts.Predicate.Limit += 1
	}

	criteria := Criteria{
		Namespace:     namespace,
		Limit:         opts.Predicate.Limit,
		LabelSelector: opts.Predicate.Label,
		FieldSelector: opts.Predicate.Field,
		PartitionID:   partitionID,
	}

	if opts.Predicate.Continue != "" {
		data, err := base64.StdEncoding.DecodeString(opts.Predicate.Continue)
		if err != nil {
			return nil, err
		}
		cont := &cont{}
		if err := json.Unmarshal(data, cont); err != nil {
			return nil, err
		}
		criteria.After = cont.ID
		criteria.ignoreCompactionCheck = criteria.After != 0
	}

	records, resourceVersionInt, err := s.db.Get(ctx, criteria)
	if err != nil {
		return nil, err
	}

	var objs []runtime.Object
	for _, rec := range records {
		obj := s.obj.DeepCopyObject()
		err := s.recordIntoObject(&rec, obj)
		if err != nil {
			return nil, err
		}
		if ok, err := opts.Predicate.Matches(obj); err != nil {
			return nil, err
		} else if ok {
			objs = append(objs, obj)
		}
	}

	if opts.Predicate.Limit != 0 && int64(len(records)) == opts.Predicate.Limit {
		data, err := json.Marshal(&cont{
			ID: records[len(records)-2].ID,
		})
		if err != nil {
			return nil, err
		}
		objs = objs[0 : len(objs)-1]
		result.Continue = base64.StdEncoding.EncodeToString(data)
		result.RemainingCount = &[]int64{1}[0]
	}

	result.ResourceVersion = strconv.FormatUint(uint64(resourceVersionInt), 10)
	result.Items = objs
	return result, nil
}

func (s *Strategy) getExisting(ctx context.Context, gvk schema.GroupVersionKind, namespace *string, name, partitionID string) (*Record, error) {
	existing, _, err := s.db.Get(ctx, Criteria{
		Name:              name,
		Namespace:         namespace,
		Limit:             1,
		NoResourceVersion: true,
		IncludeDeleted:    true,
		IncludeGC:         true,
		PartitionID:       partitionID,
	})
	if err != nil {
		return nil, err
	}
	if len(existing) == 0 {
		return nil, newNotFound(gvk, name)
	}
	return &existing[0], nil
}

func (s *Strategy) Delete(ctx context.Context, obj types.Object) (types.Object, error) {
	return s.Update(ctx, obj)
}

func (s *Strategy) UpdateStatus(ctx context.Context, obj types.Object) (types.Object, error) {
	newObj, err := s.update(ctx, true, obj)
	return newObj, translateDuplicateEntryErr(err, s.gvk, obj.GetName())
}

func (s *Strategy) Update(ctx context.Context, obj types.Object) (types.Object, error) {
	newObj, err := s.update(ctx, false, obj)
	return newObj, translateDuplicateEntryErr(err, s.gvk, obj.GetName())
}

func strptr(s string) *string {
	return &s
}

func nilOnEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func (s *Strategy) update(ctx context.Context, status bool, obj types.Object) (types.Object, error) {
	partitionID := PartitionIDFromContext(ctx)
	if s.partitionIDRequired && partitionID == "" {
		return nil, newPartitionRequiredError()
	}

	gvk, err := apiutil.GVKForObject(obj, s.scheme)
	if err != nil {
		return nil, err
	}

	existing, err := s.getExisting(ctx, gvk, strptr(obj.GetNamespace()), obj.GetName(), partitionID)
	if err != nil {
		return nil, err
	}

	if obj.GetResourceVersion() != strconv.FormatUint(uint64(existing.ID), 10) {
		return nil, newResourceVersionMismatch(gvk, obj.GetName())
	}

	if err := storage.NewUIDPreconditions(existing.UID).Check(obj.GetName(), obj); err != nil {
		return nil, newConflict(gvk, obj.GetName(), err)
	}

	newRecord, err := s.objectToRecord(obj)
	if err != nil {
		return nil, err
	}

	newRecord.Previous = &existing.ID
	newRecord.Created = existing.Created
	newRecord.Deleted = existing.Deleted
	newRecord.Removed = existing.Removed
	newRecord.UID = existing.UID
	newRecord.PartitionID = existing.PartitionID
	newRecord.Updated = time.Now()
	if status {
		newRecord.Generation = existing.Generation
		newRecord.Data = existing.Data
		newRecord.Metadata = existing.Metadata
	} else {
		if newRecord.Deleted == nil && !obj.GetDeletionTimestamp().IsZero() {
			newRecord.Deleted = &obj.GetDeletionTimestamp().Time
		}

		if newRecord.Removed == nil && newRecord.Deleted != nil && len(obj.GetFinalizers()) == 0 {
			newRecord.Removed = &newRecord.Updated
		}

		if bytes.Equal(newRecord.Metadata, existing.Metadata) && bytes.Equal(newRecord.Data, existing.Data) {
			newRecord.Generation = existing.Generation
		} else {
			newRecord.Generation = existing.Generation + 1
			newRecord.Status = existing.Status
		}
	}

	err = s.db.Insert(ctx, newRecord)
	if err != nil {
		return nil, err
	}

	return obj, s.recordIntoObject(newRecord, obj)
}

func (s *Strategy) Create(ctx context.Context, obj types.Object) (result types.Object, err error) {
	err = s.db.Transaction(ctx, func(ctx context.Context) error {
		result, err = s.create(ctx, obj)
		return translateDuplicateEntryErr(err, s.gvk, obj.GetName())
	})
	return
}

func (s *Strategy) create(ctx context.Context, obj types.Object) (types.Object, error) {
	partitionID := PartitionIDFromContext(ctx)
	if s.partitionIDRequired && partitionID == "" {
		return nil, newPartitionRequiredError()
	}

	existing, _, err := s.db.Get(ctx, Criteria{
		Name:              obj.GetName(),
		Namespace:         strptr(obj.GetNamespace()),
		Limit:             1,
		NoResourceVersion: true,
		IncludeDeleted:    true,
		IncludeGC:         true,
		PartitionID:       partitionID,
	})
	if err != nil {
		return nil, err
	}

	record, err := s.objectToRecord(obj)
	if err != nil {
		return nil, err
	}
	record.Create = true
	record.Status = nil

	if len(existing) == 1 {
		if existing[0].Removed == nil {
			return nil, newAlreadyExists(s.gvk, obj.GetName())
		}
		record.Previous = &existing[0].ID
	}

	record.PartitionID = partitionID

	err = s.db.Insert(ctx, record)
	if err != nil {
		return nil, err
	}

	return obj, s.recordIntoObject(record, obj)
}

func (s *Strategy) recordToMap(rec *Record) (map[string]any, error) {
	metadata := map[string]any{}
	data := map[string]any{}
	if len(rec.Data) > 0 {
		err := json.Unmarshal(rec.Data, &data)
		if err != nil {
			return nil, err
		}
	}

	if len(rec.Metadata) > 0 {
		err := json.Unmarshal(rec.Metadata, &metadata)
		if err != nil {
			return nil, err
		}
	}

	if len(rec.Status) > 0 {
		status := map[string]any{}

		err := json.Unmarshal(rec.Status, &status)
		if err != nil {
			return nil, err
		}

		data["status"] = status
	}

	gvk := schema.GroupVersionKind{
		Group:   rec.APIGroup,
		Version: rec.Version,
		Kind:    rec.Kind,
	}

	apiVersion, kind := gvk.ToAPIVersionAndKind()

	data["kind"] = kind
	data["apiVersion"] = apiVersion

	metadata["uid"] = rec.UID
	metadata["resourceVersion"] = strconv.Itoa(int(rec.ID))
	metadata["name"] = rec.Name
	metadata["namespace"] = rec.Namespace
	metadata["generation"] = rec.Generation
	metadata["creationTimestamp"] = rec.Created.Format(time.RFC3339)
	if rec.Deleted != nil {
		metadata["deletionTimestamp"] = rec.Deleted.Format(time.RFC3339)
	}

	data["metadata"] = metadata

	return data, nil
}

func (s *Strategy) recordIntoObject(rec *Record, obj runtime.Object) error {
	recordMap, err := s.recordToMap(rec)
	if err != nil {
		return err
	}
	d, err := json.Marshal(recordMap)
	if err != nil {
		return err
	}
	return json.Unmarshal(d, obj)
}

func (s *Strategy) objectToRecord(obj types.Object) (*Record, error) {
	gvk, err := apiutil.GVKForObject(obj, s.scheme)
	if err != nil {
		return nil, err
	}

	mapData, err := toMap(obj)
	if err != nil {
		return nil, err
	}

	status, _ := mapData["status"].(map[string]any)

	metadata, _ := mapData["metadata"].(map[string]any)
	delete(metadata, "resourceVersion")
	delete(metadata, "generation")
	delete(metadata, "uid")
	delete(metadata, "creationTimestamp")
	delete(metadata, "deletionTimestamp")
	delete(metadata, "name")
	delete(metadata, "namespace")

	metadataData, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}

	delete(mapData, "status")
	delete(mapData, "metadata")
	delete(mapData, "kind")
	delete(mapData, "apiVersion")

	specData, err := json.Marshal(mapData)
	if err != nil {
		return nil, err
	}

	statusData, err := json.Marshal(status)
	if err != nil {
		return nil, err
	}

	return &Record{
		Kind:       gvk.Kind,
		Version:    gvk.Version,
		APIGroup:   gvk.Group,
		Name:       obj.GetName(),
		Namespace:  obj.GetNamespace(),
		UID:        string(uuid.NewUUID()),
		Generation: 1,
		Previous:   nil,
		Created:    time.Now(),
		Metadata:   metadataData,
		Data:       specData,
		Status:     statusData,
	}, nil
}

func (s *Strategy) Scheme() *runtime.Scheme {
	return s.scheme
}

func toMap(obj any) (map[string]any, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	result := map[string]any{}
	return result, json.Unmarshal(data, &result)
}
