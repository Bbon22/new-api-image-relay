package controller

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	relaychannel "github.com/QuantumNous/new-api/relay/channel"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/setting/system_setting"
	"github.com/gin-gonic/gin"
)

func deliverTaskPluginImageArtifacts(ctx context.Context, task *model.Task) error {
	if task == nil || !task.PrivateData.ImageTask || task.Status != model.TaskStatusSuccess {
		return nil
	}
	store := service.GetTaskArtifactStore()
	if !store.Enabled() {
		return service.ErrTaskArtifactStoreDisabled
	}
	artifacts, err := projectTaskArtifacts(task)
	if err != nil {
		return err
	}
	imageCount := 0
	for _, artifact := range artifacts {
		if artifact.Type == "image" {
			imageCount++
		}
	}
	if imageCount == 0 {
		return errors.New("successful image task has no image artifacts")
	}
	adaptor, err := initTaskArtifactAdaptor(task)
	if err != nil {
		return err
	}
	provider, ok := adaptor.(relaychannel.TaskContentRequestProvider)
	if !ok {
		return errors.New("image task content provider is unavailable")
	}
	for _, artifact := range artifacts {
		if artifact.Type != "image" {
			continue
		}
		ref, err := store.Resolve(task, artifact.Key)
		if err != nil {
			return err
		}
		if ref != nil {
			continue
		}
		descriptor, err := provider.BuildContentRequest(ctx, task, artifact.Key, relaychannel.TaskArtifactClientRequest{Method: http.MethodGet})
		if err != nil {
			return err
		}
		if descriptor == nil {
			return errors.New("image task content request is unavailable")
		}
		if strings.HasPrefix(strings.ToLower(descriptor.URL), "data:image/") {
			dataHeader, _, found := strings.Cut(descriptor.URL, ",")
			if !found || !strings.HasSuffix(strings.ToLower(dataHeader), ";base64") {
				return errors.New("image data URL is invalid")
			}
			mimeType, _, _ := strings.Cut(strings.TrimPrefix(dataHeader, "data:"), ";")
			imageBytes, err := imageBase64Bytes(descriptor.URL)
			if err != nil {
				return err
			}
			artifact.MimeType = mimeType
			if _, err := store.Persist(ctx, task, artifact, bytes.NewReader(imageBytes)); err != nil {
				return err
			}
			continue
		}
		recorder := httptest.NewRecorder()
		ginContext, _ := gin.CreateTestContext(recorder)
		ginContext.Request = httptest.NewRequest(http.MethodGet, imageDeliveryRequestURL(), nil).WithContext(ctx)
		ginContext.Set("task_artifact_cache_key", artifact.Key)
		ginContext.Set("task_artifact_background_delivery", true)
		if err := proxyTaskMedia(ginContext, task, descriptor); err != nil {
			return err
		}
		if recorder.Code != http.StatusOK {
			return errors.New("image task content was not persisted")
		}
	}
	task.PrivateData.MediaDeliveryStatus = "succeeded"
	return task.UpdatePrivateData()
}

func imageDeliveryRequestURL() string {
	for _, address := range []string{
		system_setting.TaskPublicAddress,
		common.GetEnvOrDefaultString("TASK_PUBLIC_ADDRESS", ""),
		system_setting.ServerAddress,
	} {
		parsed, err := url.Parse(strings.TrimSpace(address))
		if err == nil && parsed.Host != "" && (parsed.Scheme == "http" || parsed.Scheme == "https") {
			return parsed.Scheme + "://" + parsed.Host + "/"
		}
	}
	return "http://localhost/"
}
