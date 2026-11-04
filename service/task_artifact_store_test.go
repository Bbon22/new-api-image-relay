package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

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
	store := newLocalArtifactStore(root)
	task := &model.Task{TaskID: "task-invalid-image", PrivateData: model.TaskPrivateData{ImageTask: true}}

	ref, err := store.Persist(t.Context(), task, types.TaskArtifact{Key: "image-1", Type: "image"}, strings.NewReader("not an image"))
	assert.Nil(t, ref)
	assert.Error(t, err)
	_, statErr := os.Stat(filepath.Join(root, task.TaskID))
	assert.ErrorIs(t, statErr, os.ErrNotExist)
}

var _ TaskArtifactStore = disabledArtifactStore{}
