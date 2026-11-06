package service

import (
	"context"
	"errors"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/system_setting"
	"github.com/QuantumNous/new-api/types"
	"github.com/gin-gonic/gin"
)

// StoredArtifactRef describes a persisted artifact object. No reference is
// produced until a concrete storage backend is implemented.
type StoredArtifactRef struct {
	Backend   string
	Bucket    string
	ObjectKey string
	MimeType  string
	Size      int64
	ExpiresAt int64
}

// TaskArtifactStore is the persistence boundary for generated artifact bytes.
// types.TaskArtifact is re-exported by relay/channel as channel.TaskArtifact.
type TaskArtifactStore interface {
	Enabled() bool
	Resolve(task *model.Task, artifactKey string) (*StoredArtifactRef, error)
	Persist(ctx context.Context, task *model.Task, artifact types.TaskArtifact, content io.Reader) (*StoredArtifactRef, error)
	Serve(c *gin.Context, task *model.Task, ref *StoredArtifactRef) error
	Delete(task *model.Task) error
}

var ErrTaskArtifactStoreDisabled = errors.New("task artifact store is disabled")
var ErrImageTaskDeliveryHandlerUnavailable = errors.New("image task delivery handler is unavailable")
var ErrImageTaskDeliveryInProgress = errors.New("image task media delivery is in progress")

const hostedArtifactCacheMaxAge int64 = 3600

func hostedArtifactCacheControl(expiresAt time.Time) string {
	maxAge := hostedArtifactCacheMaxAge
	if !expiresAt.IsZero() {
		remaining := int64(time.Until(expiresAt).Seconds())
		if remaining < maxAge {
			maxAge = remaining
		}
	}
	if maxAge < 1 {
		maxAge = 1
	}
	return "private, max-age=" + strconv.FormatInt(maxAge, 10)
}

const (
	imageTaskDeliveryWorkerCount = 64
	imageTaskDeliveryQueueSize   = 1200
)

type disabledArtifactStore struct{}

func (disabledArtifactStore) Enabled() bool {
	return false
}

func (disabledArtifactStore) Resolve(*model.Task, string) (*StoredArtifactRef, error) {
	return nil, nil
}

func (disabledArtifactStore) Persist(context.Context, *model.Task, types.TaskArtifact, io.Reader) (*StoredArtifactRef, error) {
	return nil, ErrTaskArtifactStoreDisabled
}

func (disabledArtifactStore) Serve(*gin.Context, *model.Task, *StoredArtifactRef) error {
	return ErrTaskArtifactStoreDisabled
}

func (disabledArtifactStore) Delete(*model.Task) error { return nil }

var taskArtifactStore TaskArtifactStore = &disabledArtifactStore{}
var taskArtifactStoreOnce sync.Once
var imageTaskDeliveryHandlerMu sync.RWMutex
var imageTaskDeliveryHandler func(context.Context, *model.Task) error
var imageTaskDeliveryWorkersOnce sync.Once
var imageTaskDeliveryQueue = make(chan string, imageTaskDeliveryQueueSize)
var imageTaskDeliveryQueued sync.Map
var imageTaskDeliveryLocks = struct {
	sync.Mutex
	entries map[string]*imageTaskDeliveryLock
}{entries: make(map[string]*imageTaskDeliveryLock)}

type imageTaskDeliveryLock struct {
	mu   sync.Mutex
	refs int
}

type imageTaskInlineDeliveryContextKey struct{}

// WithImageTaskInlineDelivery marks a poll round whose synchronous protocol
// response will persist the image itself if it completes before the deadline.
func WithImageTaskInlineDelivery(ctx context.Context) context.Context {
	return context.WithValue(ctx, imageTaskInlineDeliveryContextKey{}, true)
}

// RegisterImageTaskDeliveryHandler installs the controller-owned bridge that
// resolves plugin image artifacts and persists them in the host artifact store.
func RegisterImageTaskDeliveryHandler(handler func(context.Context, *model.Task) error) {
	imageTaskDeliveryHandlerMu.Lock()
	imageTaskDeliveryHandler = handler
	imageTaskDeliveryHandlerMu.Unlock()
	if handler != nil {
		imageTaskDeliveryWorkersOnce.Do(func() {
			for range imageTaskDeliveryWorkerCount {
				go runImageTaskDeliveryWorker()
			}
		})
	}
}

// QueueImageTaskMediaDelivery records successful image tasks for bounded
// background download workers. The task row is durable; customer queries can
// retry delivery if the process exits before a worker completes it.
func QueueImageTaskMediaDelivery(task *model.Task) error {
	if task == nil || !task.PrivateData.ImageTask || task.Status != model.TaskStatusSuccess || task.TaskID == "" {
		return errors.New("valid successful image task is required")
	}
	imageTaskDeliveryHandlerMu.RLock()
	handlerAvailable := imageTaskDeliveryHandler != nil
	imageTaskDeliveryHandlerMu.RUnlock()
	if !handlerAvailable {
		return ErrImageTaskDeliveryHandlerUnavailable
	}
	if _, loaded := imageTaskDeliveryQueued.LoadOrStore(task.TaskID, struct{}{}); loaded {
		return nil
	}
	select {
	case imageTaskDeliveryQueue <- task.TaskID:
		return nil
	default:
		imageTaskDeliveryQueued.Delete(task.TaskID)
		return errors.New("image task delivery queue is full")
	}
}

// DeliverImageTaskMedia downloads and stores a successful image task's output.
// The plugin boundary lives in controller to avoid a service-to-relay cycle.
func DeliverImageTaskMedia(ctx context.Context, task *model.Task) error {
	if task == nil || !task.PrivateData.ImageTask || task.Status != model.TaskStatusSuccess {
		return nil
	}
	imageTaskDeliveryHandlerMu.RLock()
	handler := imageTaskDeliveryHandler
	imageTaskDeliveryHandlerMu.RUnlock()
	if handler == nil {
		return ErrImageTaskDeliveryHandlerUnavailable
	}
	release, acquired := acquireImageTaskDeliveryLock(task.TaskID)
	if !acquired {
		return ErrImageTaskDeliveryInProgress
	}
	defer release()
	task.PrivateData.MediaDeliveryStatus = "downloading"
	if err := task.UpdatePrivateData(); err != nil {
		return err
	}
	retryBackoff := [...]time.Duration{300 * time.Millisecond, 900 * time.Millisecond}
	var deliveryErr error
	for attempt := range len(retryBackoff) + 1 {
		if attempt > 0 {
			timer := time.NewTimer(retryBackoff[attempt-1])
			select {
			case <-ctx.Done():
				timer.Stop()
				deliveryErr = ctx.Err()
			case <-timer.C:
				deliveryErr = nil
			}
			if deliveryErr != nil {
				break
			}
		}
		deliveryErr = handler(ctx, task)
		if deliveryErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			deliveryErr = ctx.Err()
			break
		}
	}
	MarkImageDeliveryFailed(task)
	return deliveryErr
}

func runImageTaskDeliveryWorker() {
	for taskID := range imageTaskDeliveryQueue {
		func() {
			defer imageTaskDeliveryQueued.Delete(taskID)
			task, exists, err := model.GetUniqueByOnlyTaskId(taskID)
			if err != nil || !exists || task == nil {
				common.SysError(common.LogText("image task delivery worker could not load task %s", taskID))
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			err = DeliverImageTaskMedia(ctx, task)
			cancel()
			if err != nil && !errors.Is(err, ErrImageTaskDeliveryInProgress) {
				common.SysError(common.LogText("image task background delivery failed for task %s", taskID))
			}
		}()
	}
}

func acquireImageTaskDeliveryLock(taskID string) (func(), bool) {
	imageTaskDeliveryLocks.Lock()
	lock := imageTaskDeliveryLocks.entries[taskID]
	if lock == nil {
		lock = &imageTaskDeliveryLock{}
		imageTaskDeliveryLocks.entries[taskID] = lock
	}
	lock.refs++
	imageTaskDeliveryLocks.Unlock()
	if !lock.mu.TryLock() {
		releaseImageTaskDeliveryLockReference(taskID, lock)
		return nil, false
	}
	return func() {
		lock.mu.Unlock()
		releaseImageTaskDeliveryLockReference(taskID, lock)
	}, true
}

func releaseImageTaskDeliveryLockReference(taskID string, lock *imageTaskDeliveryLock) {
	imageTaskDeliveryLocks.Lock()
	lock.refs--
	if lock.refs == 0 {
		delete(imageTaskDeliveryLocks.entries, taskID)
	}
	imageTaskDeliveryLocks.Unlock()
}

// GetTaskArtifactStore returns the process-wide artifact storage backend.
func GetTaskArtifactStore() TaskArtifactStore {
	taskArtifactStoreOnce.Do(func() {
		config := system_setting.LoadTaskArtifactStoreConfig()
		if config.Mode == system_setting.TaskArtifactStoreModeLocal {
			taskArtifactStore = newLocalArtifactStore(config.LocalRoot)
		}
	})
	return taskArtifactStore
}

// MarkImageDeliveryFailed records a private delivery outcome without changing
// task status or settlement. Generation may have succeeded even when the
// gateway could not download or persist the media.
func MarkImageDeliveryFailed(task *model.Task) {
	if task == nil {
		return
	}
	task.PrivateData.MediaDeliveryStatus = "failed"
	if err := task.UpdatePrivateData(); err != nil {
		common.SysError(common.LogText("failed to record image media delivery failure for task %s: %v", task.TaskID, err))
	}
}
