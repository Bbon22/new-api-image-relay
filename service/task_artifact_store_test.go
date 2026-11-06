package service

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/system_setting"
	"github.com/QuantumNous/new-api/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDisabledTaskArtifactStoreHasNoStorageBehavior(t *testing.T) {
	t.Setenv(system_setting.TaskArtifactStoreModeEnv, system_setting.TaskArtifactStoreModeUpstream)
	store := GetTaskArtifactStore()
	require.NotNil(t, store)
	assert.False(t, store.Enabled())

	task := &model.Task{TaskID: "task-disabled-store"}
	ref, err := store.Resolve(task, "video")
	require.NoError(t, err)
	assert.Nil(t, ref)

	ref, err = store.Persist(t.Context(), task, types.TaskArtifact{Key: "video", Type: "video"}, strings.NewReader("content"))
	assert.Nil(t, ref)
	assert.ErrorIs(t, err, ErrTaskArtifactStoreDisabled)
	assert.ErrorIs(t, store.Serve(&gin.Context{}, task, &StoredArtifactRef{Backend: "s3"}), ErrTaskArtifactStoreDisabled)
	assert.Same(t, store, GetTaskArtifactStore())
}

func TestLocalArtifactStoreRejectsInvalidImageBeforeCreatingTaskDirectory(t *testing.T) {
	root := t.TempDir()
	store := newLocalArtifactStore(root).(*localArtifactStore)
	task := &model.Task{TaskID: "task-invalid-image", PrivateData: model.TaskPrivateData{ImageTask: true}}

	ref, err := store.Persist(t.Context(), task, types.TaskArtifact{Key: "image-1", Type: "image"}, strings.NewReader("not an image"))
	assert.Nil(t, ref)
	assert.Error(t, err)
	_, statErr := os.Stat(filepath.Join(root, task.TaskID))
	assert.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestLocalArtifactStoreServesWithExpiryBoundedPrivateCache(t *testing.T) {
	root := t.TempDir()
	store := newLocalArtifactStore(root).(*localArtifactStore)
	task := &model.Task{
		TaskID: "task-cached-image",
		PrivateData: model.TaskPrivateData{
			ResultExpiresAt: time.Now().Add(10 * time.Minute).Unix(),
		},
	}
	path := store.artifactPath(task.TaskID, "image-1")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
	require.NoError(t, os.WriteFile(path, []byte("image-bytes"), 0o640))

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/image", nil)
	require.NoError(t, store.Serve(c, task, &StoredArtifactRef{
		Backend:   "local",
		ObjectKey: path,
		MimeType:  "image/png",
	}))

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "image/png", recorder.Header().Get("Content-Type"))
	assert.Contains(t, recorder.Header().Get("Cache-Control"), "private, max-age=")
	assert.NotContains(t, recorder.Header().Get("Cache-Control"), "no-store")
}

var _ TaskArtifactStore = disabledArtifactStore{}
