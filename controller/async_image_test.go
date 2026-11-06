package controller

import (
	"net/http/httptest"
	"testing"

	"github.com/QuantumNous/new-api/model"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestWriteImageTaskStatusReturnsFailureReason(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("GET", "/v1/tasks/task-1", nil)
	task := &model.Task{
		TaskID:     "task-1",
		Status:     model.TaskStatusFailure,
		FailReason: "请求包含当前服务不支持的参数",
	}

	writeImageTaskStatus(c, task, 1)

	require.JSONEq(t, `{"task_id":"task-1","status":"failed","message":"请求包含当前服务不支持的参数","result_url":null,"expires_at":null,"created_at":1,"finished_at":0}`, recorder.Body.String())
}

func TestResolveAsyncImageRelayPathUsesEditRelayForReferenceImageURLs(t *testing.T) {
	path, err := resolveAsyncImageRelayPath(
		"/v1/images/generations",
		"application/json",
		[]byte(`{"model":"gpt-image-2","prompt":"edit","reference_image_urls":["https://cdn.example/input.png"]}`),
	)
	require.NoError(t, err)
	require.Equal(t, "/v1/images/edits", path)
}

func TestResolveAsyncImageRelayPathKeepsTextToImageAsGeneration(t *testing.T) {
	path, err := resolveAsyncImageRelayPath(
		"/v1/images/generations",
		"application/json",
		[]byte(`{"model":"gpt-image-2","prompt":"a cat"}`),
	)
	require.NoError(t, err)
	require.Equal(t, "/v1/images/generations", path)
}

func TestResolveAsyncImageRelayPathRejectsEmptyReferenceList(t *testing.T) {
	_, err := resolveAsyncImageRelayPath(
		"/v1/images/generations",
		"application/json",
		[]byte(`{"model":"gpt-image-2","prompt":"edit","reference_image_urls":[]}`),
	)
	require.ErrorContains(t, err, "reference_image_urls")
}
