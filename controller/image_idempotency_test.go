package controller

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func setupImageIdempotencyTest(t *testing.T) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	originalDB := model.DB
	database, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, database.AutoMigrate(&model.TaskIdempotency{}))
	model.DB = database
	common.SetDatabaseTypes(common.DatabaseTypeSQLite, common.DatabaseTypeSQLite)
	t.Cleanup(func() { model.DB = originalDB })
}

func newImageIdempotencyContext(body []byte, contentType, key string) (*gin.Context, *httptest.ResponseRecorder) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", contentType)
	c.Request.Header.Set("Idempotency-Key", key)
	c.Set("token_id", 42)
	return c, recorder
}

func TestSyncImageIdempotencyReplaysStoredResponse(t *testing.T) {
	setupImageIdempotencyTest(t)
	body := []byte(`{"model":"image-model","prompt":"a cat"}`)
	c, recorder := newImageIdempotencyContext(body, "application/json", "retry-key")
	idem, handled := beginSyncImageIdempotency(c)
	require.False(t, handled)
	idem.writer.Header().Set("Content-Type", "application/json")
	idem.writer.WriteHeader(http.StatusOK)
	_, err := idem.writer.Write([]byte(`{"data":[{"url":"https://example.test/image"}]}`))
	require.NoError(t, err)
	finishSyncImageIdempotency(idem)

	replay, replayRecorder := newImageIdempotencyContext([]byte("{ \"prompt\": \"a cat\", \"model\": \"image-model\" }"), "application/json", "retry-key")
	idem, handled = beginSyncImageIdempotency(replay)
	require.True(t, handled)
	require.Nil(t, idem)
	require.Equal(t, http.StatusOK, replayRecorder.Code)
	require.JSONEq(t, `{"data":[{"url":"https://example.test/image"}]}`, replayRecorder.Body.String())
	var count int64
	require.NoError(t, model.DB.Model(&model.TaskIdempotency{}).Count(&count).Error)
	require.EqualValues(t, 1, count)
	_ = recorder
}

func TestSyncImageIdempotencyRejectsChangedPayloadAndInProgressDuplicate(t *testing.T) {
	setupImageIdempotencyTest(t)
	body := []byte(`{"model":"image-model","prompt":"a cat"}`)
	c, _ := newImageIdempotencyContext(body, "application/json", "retry-key")
	idem, handled := beginSyncImageIdempotency(c)
	require.False(t, handled)

	duplicate, duplicateRecorder := newImageIdempotencyContext(body, "application/json", "retry-key")
	_, handled = beginSyncImageIdempotency(duplicate)
	require.True(t, handled)
	require.Equal(t, http.StatusConflict, duplicateRecorder.Code)
	require.Contains(t, duplicateRecorder.Header().Get("Retry-After"), "3")
	require.NotContains(t, duplicateRecorder.Body.String(), "task_id")
	require.NoError(t, model.DB.Model(&model.TaskIdempotency{}).Where("token_id = ?", 42).Update("expires_at", 1).Error)
	var record model.TaskIdempotency
	require.NoError(t, model.DB.Where("token_id = ?", 42).First(&record).Error)
	require.NoError(t, model.RefreshTaskIdempotencyReservation(record.ID, time.Now().Add(syncImageIdempotencyTTL).Unix()))
	leasedDuplicate, leasedRecorder := newImageIdempotencyContext(body, "application/json", "retry-key")
	_, handled = beginSyncImageIdempotency(leasedDuplicate)
	require.True(t, handled)
	require.Equal(t, http.StatusConflict, leasedRecorder.Code)

	conflict, conflictRecorder := newImageIdempotencyContext([]byte(`{"model":"image-model","prompt":"a dog"}`), "application/json", "retry-key")
	_, handled = beginSyncImageIdempotency(conflict)
	require.True(t, handled)
	require.Equal(t, http.StatusConflict, conflictRecorder.Code)
	require.Contains(t, conflictRecorder.Body.String(), "idempotency_conflict")
	finishSyncImageIdempotency(idem)
}

func TestCanonicalImageMultipartPayloadIgnoresBoundary(t *testing.T) {
	build := func(boundary string) []byte {
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		require.NoError(t, writer.SetBoundary(boundary))
		require.NoError(t, writer.WriteField("prompt", "edit this image"))
		part, err := writer.CreateFormFile("image", "reference.png")
		require.NoError(t, err)
		_, err = part.Write([]byte("same image bytes"))
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		return body.Bytes()
	}

	first, err := canonicalImageMultipartPayload(build("boundary-one"), "multipart/form-data; boundary=boundary-one")
	require.NoError(t, err)
	second, err := canonicalImageMultipartPayload(build("boundary-two"), "multipart/form-data; boundary=boundary-two")
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func TestSyncImageIdempotencyRejectsStreamKey(t *testing.T) {
	setupImageIdempotencyTest(t)
	c, recorder := newImageIdempotencyContext([]byte(`{"model":"image-model","prompt":"a cat","stream":true}`), "application/json", "retry-key")
	_, handled := beginSyncImageIdempotency(c)
	require.True(t, handled)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "idempotency_stream_unsupported")
	var count int64
	require.NoError(t, model.DB.Model(&model.TaskIdempotency{}).Count(&count).Error)
	require.Zero(t, count)
}

func TestSyncImageIdempotencyReplaysSanitizedContentRejection(t *testing.T) {
	setupImageIdempotencyTest(t)
	body := []byte(`{"model":"image-model","prompt":"a cat"}`)
	first, _ := newImageIdempotencyContext(body, "application/json", "rejected-key")
	idem, handled := beginSyncImageIdempotency(first)
	require.False(t, handled)
	require.NoError(t, model.FailTaskIdempotency(idem.record.ID, string(types.ErrorCodePromptBlocked)))

	retry, recorder := newImageIdempotencyContext(body, "application/json", "rejected-key")
	_, handled = beginSyncImageIdempotency(retry)
	require.True(t, handled)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), `"message":"内容不符合规范 请调整后再试"`)
	require.NotContains(t, recorder.Body.String(), "upstream")
}
