package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/relay"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relay/helper"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/service"
	"github.com/gin-gonic/gin"
)

const (
	asyncImageQueueSize = 1200
	asyncImageWorkers   = 64
	asyncImageTimeout   = 300 * time.Second
)

var (
	asyncImageQueue chan string
	asyncImageOnce  sync.Once
)

// StartAsyncImageTaskWorker starts the durable image worker and recovery scan.
// It is safe to call more than once during tests or application startup.
func StartAsyncImageTaskWorker() {
	asyncImageOnce.Do(func() {
		asyncImageQueue = make(chan string, asyncImageQueueSize)
		for range asyncImageWorkers {
			go asyncImageWorker()
		}
		go asyncImageRecoveryLoop()
	})
}

// AsyncImage accepts an image request, persists the request and returns before
// contacting the provider. The normal synchronous endpoints are unchanged.
func AsyncImage(c *gin.Context) {
	StartAsyncImageTaskWorker()
	if asyncImageQueue != nil && len(asyncImageQueue) >= cap(asyncImageQueue)-1 {
		c.Header("Retry-After", "5")
		c.JSON(http.StatusTooManyRequests, gin.H{"error": gin.H{"message": "服务繁忙请稍后重试", "type": "new_api_error"}})
		return
	}
	requestPath := strings.TrimSuffix(c.Request.URL.Path, "/async")
	if requestPath != "/v1/images/generations" && requestPath != "/v1/images/edits" {
		c.JSON(http.StatusNotFound, gin.H{"error": gin.H{"message": "Invalid image endpoint", "type": "invalid_request_error"}})
		return
	}
	storage, err := common.GetBodyStorage(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"message": "Invalid image request", "type": "invalid_request_error"}})
		return
	}
	body, err := storage.Bytes()
	if err != nil || len(body) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"message": "Invalid image request", "type": "invalid_request_error"}})
		return
	}
	requestPath, err = resolveAsyncImageRelayPath(requestPath, c.GetHeader("Content-Type"), body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"message": err.Error(), "type": "invalid_request_error"}})
		return
	}

	// The normal image validator derives its mode from the request path. The
	// async suffix is a platform extension, so validate against the canonical
	// OpenAI path and restore the path before returning.
	originalURL := *c.Request.URL
	normalizedURL := originalURL
	normalizedURL.Path = requestPath
	normalizedURL.RawPath = ""
	c.Request.URL = &normalizedURL
	request, err := helper.GetAndValidateRequest(c, types.RelayFormatOpenAIImage)
	c.Request.URL = &originalURL
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"message": err.Error(), "type": "invalid_request_error"}})
		return
	}
	if existing, handled := reserveAsyncImageIdempotency(c, body); handled {
		if existing != nil {
			writeAsyncImageAccepted(c, existing.TaskID, "processing")
		}
		return
	}

	info, err := relaycommon.GenRelayInfo(c, types.RelayFormatOpenAIImage, request, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": gin.H{"message": "Request could not be accepted", "type": "new_api_error"}})
		return
	}
	modelName := info.OriginModelName
	if modelName == "" {
		modelName = c.GetString("original_model")
	}
	if modelName == "" {
		if imageRequest, ok := request.(*dto.ImageRequest); ok {
			modelName = imageRequest.Model
		}
	}
	if modelName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"message": "model is required", "type": "invalid_request_error"}})
		return
	}
	// Some middleware combinations can leave GenRelayInfo without a usable
	// value even though authentication and channel distribution succeeded.
	// The background relay rebuilds its own billing context, so preserve the
	// durable request using the trusted context values instead of panicking or
	// accepting a task that cannot be resumed.
	if info == nil {
		logger.LogError(c, common.LogText("async image relay info was nil; rebuilding from request context"))
		info = &relaycommon.RelayInfo{
			Request:         request,
			UserId:          common.GetContextKeyInt(c, constant.ContextKeyUserId),
			UsingGroup:      common.GetContextKeyString(c, constant.ContextKeyUsingGroup),
			UserGroup:       common.GetContextKeyString(c, constant.ContextKeyUserGroup),
			TokenId:         common.GetContextKeyInt(c, constant.ContextKeyTokenId),
			TokenKey:        common.GetContextKeyString(c, constant.ContextKeyTokenKey),
			TokenGroup:      common.GetContextKeyString(c, constant.ContextKeyTokenGroup),
			TokenUnlimited:  common.GetContextKeyBool(c, constant.ContextKeyTokenUnlimited),
			OriginModelName: modelName,
			RelayFormat:     types.RelayFormatOpenAIImage,
			StartTime:       time.Now(),
			ChannelMeta: &relaycommon.ChannelMeta{
				ChannelType:       common.GetContextKeyInt(c, constant.ContextKeyChannelType),
				ChannelId:         common.GetContextKeyInt(c, constant.ContextKeyChannelId),
				ChannelBaseUrl:    common.GetContextKeyString(c, constant.ContextKeyChannelBaseUrl),
				ApiKey:            common.GetContextKeyString(c, constant.ContextKeyChannelKey),
				UpstreamModelName: modelName,
			},
		}
	}
	if info.ChannelMeta == nil {
		info.ChannelMeta = &relaycommon.ChannelMeta{
			ChannelType:       common.GetContextKeyInt(c, constant.ContextKeyChannelType),
			ChannelId:         common.GetContextKeyInt(c, constant.ContextKeyChannelId),
			ChannelBaseUrl:    common.GetContextKeyString(c, constant.ContextKeyChannelBaseUrl),
			ApiKey:            common.GetContextKeyString(c, constant.ContextKeyChannelKey),
			UpstreamModelName: modelName,
		}
	}

	task := model.InitTask(constant.TaskPlatform("openai-image-async"), info)
	task.AsyncImage = true
	task.ImageTask = true
	task.Status = model.TaskStatusQueued
	task.Progress = "0%"
	task.Action = "image_generation"
	if requestPath == "/v1/images/edits" {
		task.Action = "image_edit"
	}
	task.Properties.OriginModelName = modelName
	task.PrivateData.ImageTask = true
	task.PrivateData.MediaDeliveryStatus = "pending"
	task.PrivateData.TokenId = c.GetInt(string(constant.ContextKeyTokenId))
	task.PrivateData.AsyncTokenGroup = common.GetContextKeyString(c, constant.ContextKeyTokenGroup)
	task.PrivateData.AsyncTokenUnlimited = common.GetContextKeyBool(c, constant.ContextKeyTokenUnlimited)
	task.PrivateData.AsyncRequestPath = requestPath
	task.PrivateData.AsyncRequestContentType = c.GetHeader("Content-Type")
	task.PrivateData.AsyncRequestBody = append([]byte(nil), body...)
	task.UserId = common.GetContextKeyInt(c, constant.ContextKeyUserId)
	task.Group = common.GetContextKeyString(c, constant.ContextKeyUsingGroup)
	task.ChannelId = common.GetContextKeyInt(c, constant.ContextKeyChannelId)
	if task.ChannelId <= 0 {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": gin.H{"message": "当前生成服务繁忙，请稍后重试", "type": "new_api_error"}})
		return
	}
	if value, ok := c.Get("async_image_idempotency"); ok {
		if record, recordOK := value.(*model.TaskIdempotency); recordOK && record != nil {
			task.TaskID = record.TaskID
		}
	}
	if err := task.InsertWithContext(c.Request.Context()); err != nil {
		completeAsyncImageIdempotency(c, "", "request_not_accepted")
		c.JSON(http.StatusInternalServerError, gin.H{"error": gin.H{"message": "Request could not be accepted", "type": "new_api_error"}})
		return
	}
	completeAsyncImageIdempotency(c, task.TaskID, "")
	enqueueAsyncImageTask(task.TaskID)
	writeAsyncImageAccepted(c, task.TaskID, "queued")
}

func resolveAsyncImageRelayPath(requestPath, contentType string, body []byte) (string, error) {
	if requestPath != "/v1/images/generations" || !strings.HasPrefix(strings.ToLower(contentType), "application/json") {
		return requestPath, nil
	}
	var payload map[string]json.RawMessage
	if err := common.Unmarshal(body, &payload); err != nil {
		return "", errors.New("Invalid image request")
	}
	rawReferences, isEdit := payload["reference_image_urls"]
	if !isEdit {
		return requestPath, nil
	}
	var references []string
	if err := common.Unmarshal(rawReferences, &references); err != nil || len(references) == 0 || len(references) > 9 {
		return "", errors.New("reference_image_urls must contain 1 to 9 image URLs")
	}
	for _, reference := range references {
		if strings.TrimSpace(reference) == "" {
			return "", errors.New("reference_image_urls must contain 1 to 9 image URLs")
		}
	}
	return "/v1/images/edits", nil
}

func writeAsyncImageAccepted(c *gin.Context, taskID, status string) {
	c.Header("Cache-Control", "private, no-store")
	c.Header("Retry-After", "3")
	c.JSON(http.StatusAccepted, gin.H{
		"task_id":               taskID,
		"status":                status,
		"message":               "图片已受理，请使用任务号查询结果",
		"poll_interval_seconds": 3,
	})
}

func reserveAsyncImageIdempotency(c *gin.Context, body []byte) (*model.TaskIdempotency, bool) {
	rawKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if rawKey == "" {
		return nil, false
	}
	if len(rawKey) > 128 || c.GetInt(string(constant.ContextKeyTokenId)) <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"message": "Idempotency-Key is invalid", "type": "invalid_request_error"}})
		return nil, true
	}
	keyDigest := sha256.Sum256([]byte(rawKey))
	hashInput := append([]byte(c.Request.Method+"\x00"+c.Request.URL.Path+"\x00"+c.GetHeader("Content-Type")+"\x00"), body...)
	bodyDigest := sha256.Sum256(hashInput)
	record := &model.TaskIdempotency{
		TokenID:     c.GetInt(string(constant.ContextKeyTokenId)),
		KeyHash:     hex.EncodeToString(keyDigest[:]),
		RequestHash: hex.EncodeToString(bodyDigest[:]),
		TaskID:      model.GenerateTaskID(),
		ExpiresAt:   time.Now().Add(10 * time.Minute).Unix(),
	}
	existing, created, err := model.ReserveTaskIdempotency(record, time.Now().Unix())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": gin.H{"message": "Request could not be accepted", "type": "new_api_error"}})
		return nil, true
	}
	if created {
		c.Set("async_image_idempotency", record)
		return nil, false
	}
	if existing.RequestHash != record.RequestHash {
		c.JSON(http.StatusConflict, gin.H{"error": gin.H{"message": "Idempotency-Key conflicts with an earlier request", "type": "invalid_request_error"}})
		return nil, true
	}
	return existing, true
}

func completeAsyncImageIdempotency(c *gin.Context, taskID, errorCode string) {
	value, ok := c.Get("async_image_idempotency")
	if !ok {
		return
	}
	record, ok := value.(*model.TaskIdempotency)
	if !ok || record == nil {
		return
	}
	state := model.TaskIdempotencyAccepted
	if taskID == "" {
		state = model.TaskIdempotencyFailed
	}
	if err := model.UpdateTaskIdempotency(record.TokenID, record.KeyHash, record.RequestHash, state, errorCode); err != nil {
		logger.LogError(c, common.LogText("failed to complete async image idempotency: %v", err))
	}
}

func enqueueAsyncImageTask(taskID string) {
	if strings.TrimSpace(taskID) == "" || asyncImageQueue == nil {
		return
	}
	select {
	case asyncImageQueue <- taskID:
	default:
		logger.LogError(context.Background(), common.LogText("async image queue is full for task %s", taskID))
	}
}

func asyncImageRecoveryLoop() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		tasks, err := model.GetPendingAsyncImageTasks(time.Now().Add(-10*time.Minute).Unix(), 100)
		if err != nil {
			logger.LogError(context.Background(), common.LogText("async image recovery query failed: %v", err))
			continue
		}
		for _, task := range tasks {
			if task != nil {
				enqueueAsyncImageTask(task.TaskID)
			}
		}
	}
}

func asyncImageWorker() {
	for taskID := range asyncImageQueue {
		runAsyncImageTask(taskID)
	}
}

func runAsyncImageTask(taskID string) {
	task, exists, err := model.GetUniqueByOnlyTaskId(taskID)
	if err != nil || !exists || task == nil || !task.AsyncImage {
		return
	}
	fromStatus := task.Status
	if fromStatus == model.TaskStatusInProgress {
		if task.UpdatedAt > 0 && task.UpdatedAt >= time.Now().Add(-10*time.Minute).Unix() {
			return
		}
		fromStatus = model.TaskStatusInProgress
	}
	if fromStatus != model.TaskStatusQueued && fromStatus != model.TaskStatusInProgress {
		return
	}
	task.Status = model.TaskStatusInProgress
	task.Progress = "1%"
	won, err := task.UpdateWithStatus(fromStatus)
	if err != nil || !won {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), asyncImageTimeout)
	defer cancel()
	if task.PrivateData.NativeAsync && strings.TrimSpace(task.PrivateData.UpstreamTaskID) != "" {
		if pollErr := pollNativeAsyncImageTask(ctx, task); pollErr != nil {
			finishAsyncImageTask(task, model.TaskStatusFailure, nil, pollErr)
		}
		return
	}
	if accepted, nativeErr := runNativeAsyncImageTask(ctx, task); accepted {
		if nativeErr != nil {
			finishAsyncImageTask(task, model.TaskStatusFailure, nil, nativeErr)
		}
		return
	}
	bg, recorder, err := newAsyncImageContext(ctx, task)
	if err == nil {
		Relay(bg, types.RelayFormatOpenAIImage)
		common.CleanupBodyStorage(bg)
	}
	if err != nil {
		finishAsyncImageTask(task, model.TaskStatusFailure, nil, err)
		return
	}
	responseBody := append([]byte(nil), recorder.Body.Bytes()...)
	if recorder.Code >= http.StatusOK && recorder.Code < http.StatusMultipleChoices {
		var response struct {
			Data []struct {
				URL string `json:"url"`
			} `json:"data"`
		}
		if common.Unmarshal(responseBody, &response) == nil && len(response.Data) > 0 && strings.TrimSpace(response.Data[0].URL) != "" {
			finishAsyncImageTask(task, model.TaskStatusSuccess, responseBody, nil)
			return
		}
	}
	if service.IsRelayImageContentRejected(recorder.Code, responseBody) || bytes.Contains(responseBody, []byte(service.TaskContentRejectedMessage)) {
		finishAsyncImageTask(task, model.TaskStatusRejected, responseBody, nil)
		return
	}
	message := service.RelayImageUpstreamErrorMessage(recorder.Code, responseBody)
	finishAsyncImageTask(task, model.TaskStatusFailure, responseBody, errors.New(message))
}

type nativeImageTaskResponse struct {
	ID       string `json:"id"`
	Status   string `json:"status"`
	Progress string `json:"progress"`
}

// runNativeAsyncImageTask uses the New API native task contract. A response
// without a task ID means the provider did not accept the job, so the caller
// may safely use the legacy synchronous fallback. Once an ID is returned, no
// second generation request is ever issued.
func runNativeAsyncImageTask(ctx context.Context, task *model.Task) (bool, error) {
	if task == nil || task.ChannelId <= 0 || len(task.PrivateData.AsyncRequestBody) == 0 {
		return false, nil
	}
	channel, err := model.CacheGetChannel(task.ChannelId)
	if err != nil || channel == nil || channel.Type != constant.ChannelTypeNewAPI {
		return false, nil
	}
	bg, _, err := newAsyncImageContext(ctx, task)
	if err != nil {
		return true, err
	}
	info, err := asyncImageBillingInfo(bg, task)
	if err != nil {
		return true, err
	}
	if billingErr := relay.PrepareRequestBilling(bg, info); billingErr != nil {
		return true, errors.New("image request billing could not be prepared")
	}
	payload, contentType, payloadErr := nativeAsyncImagePayload(ctx, task)
	if payloadErr != nil {
		task.PrivateData.FailureClass = model.TaskFailureClassLocal
		return true, payloadErr
	}
	response, body, requestErr := nativeAsyncImageRequest(ctx, task, "/v1/images/generations/async", http.MethodPost, payload, contentType)
	if requestErr != nil {
		if info.Billing != nil {
			info.Billing.Refund(bg)
		}
		return false, nil
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		if service.IsTaskContentRejection("", response.StatusCode, body) {
			prepareNativeTaskBilling(task, info)
			finishAsyncImageTask(task, model.TaskStatusRejected, nil, nil)
			settleNativeTaskBilling(bg, info, task)
			service.LogTaskConsumption(bg, info, task)
			return true, nil
		}
		if info.Billing != nil {
			info.Billing.Refund(bg)
		}
		if response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusMethodNotAllowed || response.StatusCode == http.StatusNotImplemented || response.StatusCode == http.StatusServiceUnavailable {
			return false, nil
		}
		task.PrivateData.FailureClass = model.TaskFailureClassUpstream
		message := service.RelayImageUpstreamErrorMessage(response.StatusCode, body, channel.GetBaseURL())
		return true, errors.New(message)
	}
	var submitted nativeImageTaskResponse
	if err := common.Unmarshal(body, &submitted); err != nil || strings.TrimSpace(submitted.ID) == "" {
		if info.Billing != nil {
			info.Billing.Refund(bg)
		}
		task.PrivateData.FailureClass = model.TaskFailureClassUpstream
		return false, nil
	}
	prepareNativeTaskBilling(task, info)
	task.PrivateData.NativeAsync = true
	task.PrivateData.UpstreamTaskID = submitted.ID
	task.PrivateData.AsyncRequestBody = nil
	task.PrivateData.AsyncRequestPath = ""
	task.PrivateData.AsyncRequestContentType = ""
	task.PrivateData.SettlementStatus = "succeeded"
	task.PrivateData.MediaDeliveryStatus = "pending"
	task.Status = model.TaskStatusInProgress
	task.Progress = nativeImageProgress(submitted.Status, submitted.Progress)
	if _, err := task.UpdateWithStatus(model.TaskStatusInProgress); err != nil {
		if info.Billing != nil {
			info.Billing.Refund(bg)
		}
		return true, err
	}
	if bg == nil || info == nil {
		return true, errors.New("native image billing context is unavailable")
	}
	settleNativeTaskBilling(bg, info, task)
	service.LogTaskConsumption(bg, info, task)
	return true, pollNativeAsyncImageTask(ctx, task)
}

func asyncImageBillingInfo(c *gin.Context, task *model.Task) (*relaycommon.RelayInfo, error) {
	originalURL := *c.Request.URL
	path := task.PrivateData.AsyncRequestPath
	if path == "" {
		path = "/v1/images/generations"
	}
	normalizedURL := originalURL
	normalizedURL.Path = path
	normalizedURL.RawPath = ""
	c.Request.URL = &normalizedURL
	request, err := helper.GetAndValidateRequest(c, types.RelayFormatOpenAIImage)
	c.Request.URL = &originalURL
	if err != nil {
		return nil, err
	}
	info, err := relaycommon.GenRelayInfo(c, types.RelayFormatOpenAIImage, request, nil)
	if err != nil || info == nil {
		return nil, errors.New("image relay info could not be prepared")
	}
	info.InitChannelMeta(c)
	if info.TaskRelayInfo == nil {
		info.TaskRelayInfo = &relaycommon.TaskRelayInfo{Action: task.Action}
	}
	info.ForcePreConsume = true
	return info, nil
}

func prepareNativeTaskBilling(task *model.Task, info *relaycommon.RelayInfo) {
	if task == nil || info == nil {
		return
	}
	task.Quota = info.PriceData.Quota
	if info.Billing != nil {
		task.Quota = info.Billing.GetPreConsumedQuota()
	}
	task.PrivateData.BillingSource = info.BillingSource
	task.PrivateData.SubscriptionId = info.SubscriptionId
	task.PrivateData.TokenId = info.TokenId
	task.PrivateData.BillingContext = &model.TaskBillingContext{
		ModelPrice:      info.PriceData.ModelPrice,
		GroupRatio:      info.PriceData.GroupRatioInfo.GroupRatio,
		ModelRatio:      info.PriceData.ModelRatio,
		OtherRatios:     info.PriceData.OtherRatios(),
		OriginModelName: info.OriginModelName,
		PerCallBilling:  common.StringsContains(constant.TaskPricePatches, info.OriginModelName) || info.PriceData.UsePrice,
		TieredSnapshot:  info.TieredBillingSnapshot,
	}
}

func settleNativeTaskBilling(ctx *gin.Context, info *relaycommon.RelayInfo, task *model.Task) {
	if info == nil || info.Billing == nil || task == nil {
		return
	}
	if err := service.SettleBilling(ctx, info, task.Quota); err != nil {
		logger.LogError(ctx, common.LogText("native async image billing settlement failed for task %s: %v", task.TaskID, err))
	}
}

func nativeAsyncImageRequest(ctx context.Context, task *model.Task, path, method string, body []byte, contentType string) (*http.Response, []byte, error) {
	channel, err := model.CacheGetChannel(task.ChannelId)
	if err != nil || channel == nil {
		return nil, nil, errors.New("image channel is unavailable")
	}
	requestURL := relaycommon.GetFullRequestURL(channel.GetBaseURL(), path, channel.Type)
	request, err := http.NewRequestWithContext(ctx, method, requestURL, bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	request.Header.Set("Authorization", "Bearer "+task.PrivateData.Key)
	if len(body) > 0 {
		if contentType == "" {
			contentType = "application/json"
		}
		request.Header.Set("Content-Type", contentType)
	}
	request.Header.Set("Accept", "application/json")
	client, err := service.GetHttpClientWithProxySettings(channel.GetSetting().Proxy, channel.GetSetting())
	if err != nil {
		return nil, nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, nil, err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 128<<20))
	if err != nil {
		return response, nil, err
	}
	return response, responseBody, nil
}

func pollNativeAsyncImageTask(ctx context.Context, task *model.Task) error {
	deadline := time.Now().Add(asyncImageTimeout)
	upstreamURL := ""
	if channel, err := model.CacheGetChannel(task.ChannelId); err == nil && channel != nil {
		upstreamURL = channel.GetBaseURL()
	}
	for {
		if time.Now().After(deadline) {
			return errors.New("native image task polling timed out")
		}
		response, body, err := nativeAsyncImageRequest(ctx, task, "/v1/images/generations/"+task.PrivateData.UpstreamTaskID, http.MethodGet, nil, "")
		if err != nil || response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError {
			if err := waitNativeImagePoll(ctx, 3*time.Second); err != nil {
				return err
			}
			continue
		}
		if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
			if service.IsTaskContentRejection("", response.StatusCode, body) {
				finishAsyncImageTask(task, model.TaskStatusRejected, nil, nil)
			} else {
				task.PrivateData.FailureClass = model.TaskFailureClassUpstream
				return errors.New(service.RelayImageUpstreamErrorMessage(response.StatusCode, body, upstreamURL))
			}
			return nil
		}
		var current nativeImageTaskResponse
		if err := common.Unmarshal(body, &current); err != nil {
			return errors.New("native image task response is invalid")
		}
		switch strings.ToLower(strings.TrimSpace(current.Status)) {
		case "succeeded", "success", "completed":
			finishNativeImageSuccess(ctx, task, body)
			return nil
		case "rejected":
			finishAsyncImageTask(task, model.TaskStatusRejected, nil, nil)
			return nil
		case "failed", "failure", "error":
			if service.IsTaskContentRejection("", response.StatusCode, body) {
				finishAsyncImageTask(task, model.TaskStatusRejected, nil, nil)
			} else {
				task.PrivateData.FailureClass = model.TaskFailureClassUpstream
				message := service.RelayImageUpstreamErrorMessage(response.StatusCode, body, upstreamURL)
				finishAsyncImageTask(task, model.TaskStatusFailure, nil, errors.New(message))
			}
			return nil
		default:
			task.Status = model.TaskStatusInProgress
			task.Progress = nativeImageProgress(current.Status, current.Progress)
			_, _ = task.UpdateWithStatus(model.TaskStatusInProgress)
			if err := waitNativeImagePoll(ctx, 3*time.Second); err != nil {
				return err
			}
		}
	}
}

// nativeAsyncImagePayload converts every accepted async edit form into the
// upstream native JSON contract. Reference media is always validated and
// hosted by this service first, so the provider sees only our short-lived URL.
func nativeAsyncImagePayload(ctx context.Context, task *model.Task) ([]byte, string, error) {
	if task == nil || len(task.PrivateData.AsyncRequestBody) == 0 {
		return nil, "", errors.New("async image request is missing")
	}
	contentType := strings.TrimSpace(task.PrivateData.AsyncRequestContentType)
	if strings.HasPrefix(strings.ToLower(contentType), "application/json") {
		return hostNativeJSONReferences(ctx, task.PrivateData.AsyncRequestBody)
	}
	if !strings.HasPrefix(strings.ToLower(contentType), "multipart/form-data") {
		return nil, "", errors.New("async image request must use JSON or multipart form")
	}
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil || mediaType != "multipart/form-data" || params["boundary"] == "" {
		return nil, "", errors.New("invalid async image form")
	}
	reader := multipart.NewReader(bytes.NewReader(task.PrivateData.AsyncRequestBody), params["boundary"])
	fields := make(map[string]any)
	var references []string
	for {
		part, nextErr := reader.NextPart()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			return nil, "", errors.New("invalid async image form")
		}
		name := strings.TrimSpace(part.FormName())
		if name == "" {
			_ = part.Close()
			continue
		}
		if isAsyncReferenceField(name) {
			if part.FileName() != "" {
				data, readErr := io.ReadAll(io.LimitReader(part, 8<<20+1))
				_ = part.Close()
				if readErr != nil || len(data) > 8<<20 {
					return nil, "", errors.New("reference image is invalid")
				}
				if err := service.ValidateRelayImageReferenceBytes(data); err != nil {
					return nil, "", errors.New("reference image is invalid")
				}
				url, hostErr := service.StoreRelayImageReference(ctx, data)
				if hostErr != nil {
					return nil, "", errors.New("reference image could not be hosted")
				}
				references = append(references, url)
				continue
			}
			value, readErr := io.ReadAll(io.LimitReader(part, 8<<20+1))
			_ = part.Close()
			if readErr != nil || len(value) > 8<<20 {
				return nil, "", errors.New("reference image is invalid")
			}
			url, hostErr := service.HostRelayImageReference(ctx, string(value))
			if hostErr != nil {
				return nil, "", errors.New("reference image could not be hosted")
			}
			references = append(references, url)
			continue
		}
		if part.FileName() != "" {
			_ = part.Close()
			return nil, "", errors.New("async image form contains an unsupported file field")
		}
		value, readErr := io.ReadAll(io.LimitReader(part, 1<<20+1))
		_ = part.Close()
		if readErr != nil || len(value) > 1<<20 {
			return nil, "", errors.New("async image form is too large")
		}
		fields[name] = nativeAsyncScalar(name, string(value))
	}
	if len(references) > 0 {
		fields["reference_image_urls"] = references
	}
	if len(fields) == 0 {
		return nil, "", errors.New("async image request is empty")
	}
	fields["response_format"] = "url"
	body, err := json.Marshal(fields)
	if err != nil {
		return nil, "", errors.New("async image request is invalid")
	}
	return body, "application/json", nil
}

func hostNativeJSONReferences(ctx context.Context, body []byte) ([]byte, string, error) {
	var fields map[string]json.RawMessage
	if err := common.Unmarshal(body, &fields); err != nil || fields == nil {
		return nil, "", errors.New("async image request is invalid")
	}
	var inputs []string
	for _, key := range []string{"reference_image_urls", "image", "images"} {
		raw, ok := fields[key]
		if !ok {
			continue
		}
		var list []string
		if err := common.Unmarshal(raw, &list); err != nil {
			var single string
			if err := common.Unmarshal(raw, &single); err != nil || strings.TrimSpace(single) == "" {
				return nil, "", errors.New("reference_image_urls must contain image URLs")
			}
			list = []string{single}
		}
		inputs = append(inputs, list...)
		delete(fields, key)
	}
	if len(inputs) > 9 {
		return nil, "", errors.New("reference_image_urls must contain 1 to 9 image URLs")
	}
	if len(inputs) > 0 {
		hosted := make([]string, 0, len(inputs))
		for _, input := range inputs {
			url, err := service.HostRelayImageReference(ctx, input)
			if err != nil {
				return nil, "", errors.New("reference image could not be hosted")
			}
			hosted = append(hosted, url)
		}
		hostedJSON, err := json.Marshal(hosted)
		if err != nil {
			return nil, "", errors.New("async image request is invalid")
		}
		fields["reference_image_urls"] = hostedJSON
	}
	fields["response_format"] = json.RawMessage(`"url"`)
	normalized, err := json.Marshal(fields)
	if err != nil {
		return nil, "", errors.New("async image request is invalid")
	}
	return normalized, "application/json", nil
}

func isAsyncReferenceField(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	return name == "image" || name == "image[]" || name == "images" || name == "images[]" ||
		name == "reference_image" || name == "reference_images" || name == "reference_images[]" ||
		strings.HasPrefix(name, "image[") || name == "reference_image_urls"
}

func nativeAsyncScalar(name, value string) any {
	trimmed := strings.TrimSpace(value)
	switch strings.ToLower(name) {
	case "n", "output_compression", "partial_images":
		if parsed, err := strconv.Atoi(trimmed); err == nil {
			return parsed
		}
	case "stream", "watermark":
		if parsed, err := strconv.ParseBool(trimmed); err == nil {
			return parsed
		}
	}
	return value
}

func waitNativeImagePoll(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nativeImageProgress(status, progress string) string {
	if strings.TrimSpace(progress) != "" {
		return progress
	}
	if strings.EqualFold(strings.TrimSpace(status), "queued") {
		return "1%"
	}
	return "50%"
}

func finishNativeImageSuccess(ctx context.Context, task *model.Task, responseBody []byte) {
	if task == nil {
		return
	}
	rewritten, err := service.RewriteRelayImageResponse(ctx, responseBody, "application/json")
	if err != nil {
		task.Status = model.TaskStatusSuccess
		task.FinishTime = time.Now().Unix()
		task.Progress = "99%"
		task.Data = responseBody
		task.PrivateData.ResultURL = ""
		task.PrivateData.MediaDeliveryStatus = "failed"
		task.PrivateData.SettlementStatus = "succeeded"
		task.PrivateData.ResultExpiresAt = time.Now().Add(time.Duration(model.TaskResultRetentionSeconds) * time.Second).Unix()
		if _, updateErr := task.UpdateWithStatus(model.TaskStatusInProgress); updateErr != nil {
			logger.LogError(ctx, common.LogText("native async image delivery update failed for task %s: %v", task.TaskID, updateErr))
		}
		logger.LogWarn(ctx, common.LogText("native async image media delivery failed for task %s: %v", task.TaskID, err))
		return
	}
	var result struct {
		Data []struct {
			URL string `json:"url"`
		} `json:"data"`
	}
	if common.Unmarshal(rewritten, &result) != nil || len(result.Data) == 0 || strings.TrimSpace(result.Data[0].URL) == "" {
		finishAsyncImageTask(task, model.TaskStatusFailure, nil, errors.New("native image result is missing"))
		return
	}
	task.Status = model.TaskStatusSuccess
	task.FinishTime = time.Now().Unix()
	task.Progress = "100%"
	task.Data = rewritten
	task.PrivateData.ResultURL = result.Data[0].URL
	task.PrivateData.MediaDeliveryStatus = "succeeded"
	task.PrivateData.SettlementStatus = "succeeded"
	task.PrivateData.ResultExpiresAt = time.Now().Add(time.Duration(model.TaskResultRetentionSeconds) * time.Second).Unix()
	if _, err := task.UpdateWithStatus(model.TaskStatusInProgress); err != nil {
		logger.LogError(ctx, common.LogText("native async image success update failed for task %s: %v", task.TaskID, err))
	}
}

func newAsyncImageContext(ctx context.Context, task *model.Task) (*gin.Context, *httptest.ResponseRecorder, error) {
	if task == nil || len(task.PrivateData.AsyncRequestBody) == 0 || task.PrivateData.AsyncRequestPath == "" {
		return nil, nil, errors.New("async image request is missing")
	}
	recorder := httptest.NewRecorder()
	bg, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPost, task.PrivateData.AsyncRequestPath, bytes.NewReader(task.PrivateData.AsyncRequestBody)).WithContext(ctx)
	request.Header.Set("Content-Type", task.PrivateData.AsyncRequestContentType)
	bg.Request = request
	bg.Set("id", task.UserId)
	bg.Set("async_image_channel_id", task.ChannelId)
	common.SetContextKey(bg, constant.ContextKeyUserId, task.UserId)
	common.SetContextKey(bg, constant.ContextKeyUsingGroup, task.Group)
	common.SetContextKey(bg, constant.ContextKeyUserGroup, task.Group)
	common.SetContextKey(bg, constant.ContextKeyTokenGroup, task.PrivateData.AsyncTokenGroup)
	common.SetContextKey(bg, constant.ContextKeyTokenId, task.PrivateData.TokenId)
	common.SetContextKey(bg, constant.ContextKeyTokenUnlimited, task.PrivateData.AsyncTokenUnlimited)
	common.SetContextKey(bg, constant.ContextKeyOriginalModel, task.Properties.OriginModelName)
	common.SetContextKey(bg, constant.ContextKeyRequestStartTime, time.Now())
	if task.PrivateData.TokenId > 0 {
		token, err := model.GetTokenById(task.PrivateData.TokenId)
		if err != nil || token == nil || token.UserId != task.UserId {
			return nil, nil, errors.New("image token is no longer available")
		}
		common.SetContextKey(bg, constant.ContextKeyTokenKey, token.Key)
		common.SetContextKey(bg, constant.ContextKeyTokenUnlimited, token.UnlimitedQuota)
	}
	channel, err := model.CacheGetChannel(task.ChannelId)
	if err != nil || channel == nil {
		return nil, nil, errors.New("image channel is no longer available")
	}
	key := task.PrivateData.Key
	if key == "" {
		var keyErr error
		key, _, keyErr = channel.GetNextEnabledKey()
		if keyErr != nil {
			return nil, nil, errors.New("image channel has no available key")
		}
	}
	common.SetContextKey(bg, constant.ContextKeyChannelId, channel.Id)
	common.SetContextKey(bg, constant.ContextKeyChannelName, channel.Name)
	common.SetContextKey(bg, constant.ContextKeyChannelType, channel.Type)
	common.SetContextKey(bg, constant.ContextKeyChannelBaseUrl, channel.GetBaseURL())
	common.SetContextKey(bg, constant.ContextKeyChannelKey, key)
	common.SetContextKey(bg, constant.ContextKeyChannelAutoBan, channel.GetAutoBan())
	common.SetContextKey(bg, constant.ContextKeyChannelModelMapping, channel.GetModelMapping())
	common.SetContextKey(bg, constant.ContextKeyChannelStatusCodeMapping, channel.GetStatusCodeMapping())
	common.SetContextKey(bg, constant.ContextKeyChannelSetting, channel.GetSetting())
	common.SetContextKey(bg, constant.ContextKeyChannelOtherSetting, channel.GetOtherSettings())
	return bg, recorder, nil
}

func finishAsyncImageTask(task *model.Task, status model.TaskStatus, responseBody []byte, runErr error) {
	if task == nil {
		return
	}
	task.Status = status
	task.FinishTime = time.Now().Unix()
	task.Progress = "100%"
	task.Data = nil
	task.PrivateData.AsyncRequestBody = nil
	task.PrivateData.AsyncRequestPath = ""
	task.PrivateData.AsyncRequestContentType = ""
	switch status {
	case model.TaskStatusSuccess:
		task.Data = responseBody
		task.PrivateData.MediaDeliveryStatus = "succeeded"
		task.PrivateData.SettlementStatus = "succeeded"
		task.PrivateData.ResultExpiresAt = time.Now().Add(time.Duration(model.TaskResultRetentionSeconds) * time.Second).Unix()
		var response struct {
			Data []struct {
				URL string `json:"url"`
			} `json:"data"`
		}
		if common.Unmarshal(responseBody, &response) == nil && len(response.Data) > 0 {
			task.PrivateData.ResultURL = response.Data[0].URL
		}
	case model.TaskStatusRejected:
		task.FailReason = service.TaskContentRejectedMessage
		task.PrivateData.SettlementStatus = "charged"
		task.PrivateData.MediaDeliveryStatus = "failed"
	case model.TaskStatusFailure:
		task.FailReason = "图片生成失败，请稍后重试"
		if runErr != nil {
			if message := service.SanitizeRelayImageErrorMessage(runErr.Error()); message != "" {
				task.FailReason = message
			}
		}
		task.PrivateData.SettlementStatus = "failed"
		task.PrivateData.MediaDeliveryStatus = "failed"
		if runErr != nil {
			logger.LogError(context.Background(), common.LogText("async image task %s failed: %v", task.TaskID, runErr))
		}
	}
	if won, err := task.UpdateWithStatus(model.TaskStatusInProgress); err != nil || !won {
		if err != nil {
			logger.LogError(context.Background(), common.LogText("async image task %s update failed: %v", task.TaskID, err))
		}
		return
	}
	if status == model.TaskStatusFailure && task.Quota != 0 {
		service.RefundTaskQuota(context.Background(), task, task.FailReason)
	}
}

// retryNativeAsyncImageDelivery retries only the local media transformation
// for an already successful upstream task. It never submits or regenerates an
// image, and therefore never creates a second charge.
func retryNativeAsyncImageDelivery(ctx context.Context, task *model.Task) bool {
	if task == nil || !task.AsyncImage || !task.PrivateData.NativeAsync || task.Status != model.TaskStatusSuccess ||
		task.PrivateData.MediaDeliveryStatus != "failed" || len(task.Data) == 0 {
		return false
	}
	rewritten, err := service.RewriteRelayImageResponse(ctx, task.Data, "application/json")
	if err != nil {
		return false
	}
	var result struct {
		Data []struct {
			URL string `json:"url"`
		} `json:"data"`
	}
	if common.Unmarshal(rewritten, &result) != nil || len(result.Data) == 0 || strings.TrimSpace(result.Data[0].URL) == "" {
		return false
	}
	task.Data = rewritten
	task.PrivateData.ResultURL = result.Data[0].URL
	task.PrivateData.MediaDeliveryStatus = "succeeded"
	if task.PrivateData.SettlementStatus == "" {
		task.PrivateData.SettlementStatus = "succeeded"
	}
	if task.PrivateData.ResultExpiresAt == 0 {
		task.PrivateData.ResultExpiresAt = time.Now().Add(time.Duration(model.TaskResultRetentionSeconds) * time.Second).Unix()
	}
	if err := task.UpdateDataAndPrivateData(); err != nil {
		logger.LogError(ctx, common.LogText("native async image redelivery update failed for task %s: %v", task.TaskID, err))
		return false
	}
	return true
}
