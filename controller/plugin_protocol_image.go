package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
	pluginruntime "github.com/QuantumNous/new-api/pkg/jsplugin"
	"github.com/QuantumNous/new-api/relay"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/service"
	roottype "github.com/QuantumNous/new-api/types"
	"github.com/gin-gonic/gin"
)

var errImageTaskStillProcessing = errors.New("image task is still processing")

const imageClientWaitTimeout = 60 * time.Second

// serveTaskPluginImageProtocol answers an OpenAI Images API request that a
// task plugin claimed. The API is synchronous: the durable submission runs
// first on a context that survives a client disconnect, an asynchronous
// upstream task is then polled inside the request until it is terminal or the
// protocol timeout elapses, and the plugin's render hook finally shapes the
// OpenAI image response from the terminal task.
func serveTaskPluginImageProtocol(c *gin.Context, pinned pluginruntime.PinnedEndpoint, deps pluginProtocolBridgeDeps) {
	deps = deps.withDefaults()
	requestValue, exists := c.Get(pluginruntime.ContextKeyProtocolRequest)
	protocolRequest, ok := requestValue.(pluginruntime.ProtocolRequestContext)
	if !exists || !ok || protocolRequest.Protocol != pinned.Protocol || pinned.Plugin == nil {
		respondPluginProtocolError(c, http.StatusInternalServerError, "task_protocol_error", "Task protocol request failed")
		return
	}
	if !deps.artifactStorageEnabled() {
		respondImageMediaDeliveryError(c, "")
		return
	}
	clientRequest := c.Request
	c.Set("hosted_image_protocol", true)
	imageRequestDeadline := time.Now().Add(imageClientWaitTimeout)
	var relayInfo *relaycommon.RelayInfo
	var relayInfoErr error
	var outcome *taskSubmissionOutcome
	var taskErr *dto.TaskError
	idempotencyReservation, duplicateResponse := reserveImageIdempotency(c, protocolRequest)
	if duplicateResponse {
		return
	}
	// DashScope bills the images as soon as it accepts the upstream call. A
	// client that gives up while the submission is in flight must not cancel
	// that call, the task row, its settlement, or its consume log, so the
	// submission runs on its own bounded context exactly like the Responses
	// bridge. Only the in-request wait and the response write observe the
	// client connection.
	func() {
		submissionTimeout := min(deps.submissionTimeout, time.Until(imageRequestDeadline))
		if submissionTimeout <= 0 {
			return
		}
		submissionContext, cancelSubmission := context.WithTimeout(
			context.WithoutCancel(clientRequest.Context()),
			submissionTimeout,
		)
		c.Request = clientRequest.Clone(submissionContext)
		defer func() {
			c.Request = clientRequest
			cancelSubmission()
		}()

		relayInfo, relayInfoErr = relaycommon.GenRelayInfo(c, types.RelayFormatTask, nil, nil)
		if relayInfoErr != nil {
			return
		}
		if idempotencyReservation != nil {
			if relayInfo.TaskRelayInfo == nil {
				relayInfo.TaskRelayInfo = &relaycommon.TaskRelayInfo{}
			}
			relayInfo.TaskRelayInfo.PublicTaskID = idempotencyReservation.TaskID
		}
		relayInfo.IsStream = false
		relayInfo.OriginModelName = c.GetString("resolved_task_model")
		if action := c.GetString("task_action"); action != "" {
			relayInfo.Action = action
		}
		if taskErr = relay.ResolveOriginTask(c, relayInfo); taskErr != nil {
			return
		}
		if taskErr = relay.ApplyOriginTaskAffinity(c, relayInfo); taskErr != nil {
			return
		}
		outcome, taskErr = deps.submit(c, relayInfo)
	}()
	if clientRequest.Context().Err() != nil {
		// The client is gone. A durable task keeps its settlement and consume
		// log; an asynchronous one is finished by the background poller.
		if outcome != nil && outcome.Task != nil && outcome.Task.ID > 0 &&
			outcome.Task.PrivateData.ImageTask && outcome.Task.Status == model.TaskStatusSuccess {
			outcome.Task.PrivateData.ResultDiscarded = true
			if err := outcome.Task.UpdatePrivateData(); err != nil {
				logger.LogError(c, common.LogText("failed to mark disconnected image result as discarded"))
			}
		}
		return
	}
	if relayInfoErr != nil {
		completeImageIdempotency(idempotencyReservation, "", "task_protocol_error")
		logger.LogError(c, common.LogText("build task protocol relay info failed: %s", relayInfoErr.Error()))
		respondPluginProtocolError(c, http.StatusInternalServerError, "task_protocol_error", "Task protocol request failed")
		return
	}
	if taskErr != nil {
		completeImageIdempotency(idempotencyReservation, "", taskErr.Code)
		respondTaskPluginImageError(c, taskErr)
		return
	}
	if outcome == nil || outcome.Task == nil || outcome.Task.Platform != constant.TaskPlatform(pinned.Plugin.Meta.Key) {
		completeImageIdempotency(idempotencyReservation, "", "task_protocol_error")
		logger.LogError(c, common.LogText("task protocol submission returned an invalid durable outcome"))
		respondPluginProtocolError(c, http.StatusInternalServerError, "task_protocol_error", "Task protocol request failed")
		return
	}
	task := outcome.Task
	completeImageIdempotency(idempotencyReservation, task.TaskID, "")
	if task.Status != model.TaskStatusSuccess && task.Status != model.TaskStatusFailure && task.Status != model.TaskStatusRejected {
		if taskErr = waitTaskPluginImageTask(c, task, deps, time.Until(imageRequestDeadline)); taskErr != nil {
			if errors.Is(taskErr.Error, errImageTaskStillProcessing) && c.Request.Context().Err() == nil {
				respondImageTaskAccepted(c, task)
				return
			}
			if c.Request.Context().Err() == nil {
				respondTaskPluginImageError(c, taskErr)
			}
			return
		}
	}
	if task.Status == model.TaskStatusFailure {
		// Never reflect a provider reason, URL, or status to the customer.
		taskErr = service.TaskErrorWrapperLocal(errors.New("图片生成失败，请稍后重试"), "image_generation_failed", http.StatusBadGateway)
		taskErr.LocalError = true
		respondTaskPluginImageError(c, taskErr)
		return
	}
	if task.Status == model.TaskStatusRejected {
		taskErr = service.TaskErrorWrapperLocal(errors.New(service.TaskContentRejectedMessage), "content_rejected", http.StatusBadRequest)
		respondTaskPluginImageError(c, taskErr)
		return
	}
	view, err := service.BuildTaskPluginView(task)
	if err != nil {
		logger.LogError(c, common.LogText("build task protocol view failed: %s", err.Error()))
		respondPluginProtocolError(c, http.StatusInternalServerError, "task_protocol_error", "Task protocol request failed")
		return
	}
	viewValue, err := taskPluginProtocolJSONValue(view)
	if err != nil {
		logger.LogError(c, common.LogText("encode task protocol view failed: %s", err.Error()))
		respondPluginProtocolError(c, http.StatusInternalServerError, "task_protocol_error", "Task protocol request failed")
		return
	}
	payload, err := pinned.Plugin.Engine.CallPathWithAdmissionTimeout(c.Request.Context(), deps.admissionTimeout, "protocols", []string{pinned.Protocol, "render"}, protocolRequest.JSValue(), viewValue)
	if err != nil {
		logger.LogError(c, common.LogText("task protocol image render hook failed: %s", err.Error()))
		respondPluginProtocolError(c, http.StatusInternalServerError, "task_protocol_error", "Task protocol request failed")
		return
	}
	response, ok := payload.(map[string]any)
	data, hasData := response["data"].([]any)
	if !ok || !hasData {
		logger.LogError(c, common.LogText("task protocol image render hook must return an object with a data array"))
		respondPluginProtocolError(c, http.StatusInternalServerError, "task_protocol_error", "Task protocol request failed")
		return
	}
	if _, present := response["created"]; !present {
		createdAt := task.CreatedAt
		if createdAt == 0 {
			createdAt = task.SubmitTime
		}
		response["created"] = createdAt
	}
	// Persist every image before returning it. The client sees only a signed
	// URL hosted by this service, never the provider's URL.
	responseFormat := taskPluginImageResponseFormat(protocolRequest)
	imageIndex := 0
	for _, entry := range data {
		item, isObject := entry.(map[string]any)
		if !isObject {
			respondImageMediaDeliveryError(c, task.TaskID)
			return
		}
		imageURL, _ := item["url"].(string)
		encoded, _ := item["b64_json"].(string)
		mimeType, _ := item["mime_type"].(string)
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(imageURL)), "data:image/") {
			if dataHeader, _, ok := strings.Cut(imageURL, ","); ok {
				if declaredType, _, found := strings.Cut(strings.TrimPrefix(strings.ToLower(dataHeader), "data:"), ";"); found {
					mimeType = declaredType
				}
			}
			encoded = imageURL
		} else if strings.TrimSpace(imageURL) != "" {
			var downloadErr error
			mimeType, encoded, downloadErr = downloadImageWithRetry(c.Request.Context(), deps, imageURL)
			if downloadErr != nil {
				logger.LogWarn(c, common.LogText("task protocol image delivery download failed for task %s", task.TaskID))
				service.MarkImageDeliveryFailed(task)
				respondImageMediaDeliveryError(c, task.TaskID)
				return
			}
		} else if strings.TrimSpace(encoded) == "" && task.PrivateData.ImageTask && strings.HasPrefix(task.PrivateData.ResultURL, "data:image/") {
			encoded = task.PrivateData.ResultURL
		}
		if strings.TrimSpace(encoded) == "" {
			respondImageMediaDeliveryError(c, task.TaskID)
			return
		}
		imageBytes, decodeErr := imageBase64Bytes(encoded)
		if decodeErr != nil {
			logger.LogWarn(c, common.LogText("task protocol image delivery data invalid for task %s", task.TaskID))
			service.MarkImageDeliveryFailed(task)
			respondImageMediaDeliveryError(c, task.TaskID)
			return
		}
		encoded = base64.StdEncoding.EncodeToString(imageBytes)
		imageIndex++
		artifactKey := fmt.Sprintf("image-%d", imageIndex)
		hostedURL, persistErr := deps.persistImage(c.Request.Context(), task, artifactKey, mimeType, encoded)
		if persistErr != nil || strings.TrimSpace(hostedURL) == "" {
			logger.LogWarn(c, common.LogText("task protocol image delivery persistence failed for task %s", task.TaskID))
			service.MarkImageDeliveryFailed(task)
			respondImageMediaDeliveryError(c, task.TaskID)
			return
		}
		item["url"] = hostedURL
		if responseFormat == "b64_json" {
			item["b64_json"] = encoded
		}
	}
	if imageIndex == 0 {
		service.MarkImageDeliveryFailed(task)
		respondImageMediaDeliveryError(c, task.TaskID)
		return
	}
	c.JSON(http.StatusOK, response)
}

func downloadImageWithRetry(ctx context.Context, deps pluginProtocolBridgeDeps, imageURL string) (string, string, error) {
	var mimeType, encoded string
	var downloadErr error
	backoff := [...]time.Duration{300 * time.Millisecond, 900 * time.Millisecond}
	for attempt := 0; attempt < 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return "", "", err
		}
		mimeType, encoded, downloadErr = deps.downloadImage(imageURL)
		if downloadErr == nil && strings.TrimSpace(encoded) != "" {
			return mimeType, encoded, nil
		}
		if attempt == len(backoff) {
			break
		}
		timer := time.NewTimer(backoff[attempt])
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", "", ctx.Err()
		case <-timer.C:
		}
	}
	if downloadErr == nil {
		downloadErr = errors.New("downloaded image is empty")
	}
	return "", "", downloadErr
}

func persistHostedImageTaskResult(ctx context.Context, task *model.Task, artifactKey, mimeType, encoded string) (string, error) {
	store := service.GetTaskArtifactStore()
	if !store.Enabled() {
		return "", service.ErrTaskArtifactStoreDisabled
	}
	imageBytes, err := imageBase64Bytes(encoded)
	if err != nil {
		return "", err
	}
	ref, err := store.Persist(ctx, task, roottype.TaskArtifact{Key: artifactKey, Type: "image", MimeType: mimeType}, bytes.NewReader(imageBytes))
	if err != nil {
		return "", err
	}
	if ref == nil {
		return "", errors.New("image artifact was not persisted")
	}
	task.PrivateData.MediaDeliveryStatus = "succeeded"
	if err := task.UpdatePrivateData(); err != nil {
		return "", err
	}
	return service.BuildTaskArtifactContentURL(task.TaskID, artifactKey, ref.ExpiresAt)
}

func imageBase64Bytes(encoded string) ([]byte, error) {
	encoded = strings.TrimSpace(encoded)
	if _, after, ok := strings.Cut(encoded, ","); ok && strings.HasPrefix(strings.ToLower(encoded), "data:") {
		encoded = after
	}
	imageBytes, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		imageBytes, err = base64.RawStdEncoding.DecodeString(encoded)
	}
	if err != nil || len(imageBytes) == 0 || len(imageBytes) > 64<<20 {
		return nil, errors.New("invalid image data")
	}
	return imageBytes, nil
}

func respondImageMediaDeliveryError(c *gin.Context, taskID string) {
	c.Header("Cache-Control", "private, no-store")
	body := gin.H{"error": gin.H{
		"message": "媒体交付失败",
		"type":    "new_api_error",
		"code":    "media_delivery_failed",
	}}
	if strings.TrimSpace(taskID) != "" {
		body["task_id"] = taskID
	}
	c.JSON(http.StatusBadGateway, body)
}

func reserveImageIdempotency(c *gin.Context, protocolRequest pluginruntime.ProtocolRequestContext) (*model.TaskIdempotency, bool) {
	rawKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if rawKey == "" {
		return nil, false
	}
	if len(rawKey) > 128 || c.GetInt("token_id") <= 0 {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": gin.H{
			"message": "Idempotency-Key is invalid",
			"type":    "invalid_request_error",
			"code":    "invalid_idempotency_key",
		}})
		return nil, true
	}
	storage, err := common.GetBodyStorage(c)
	if err != nil {
		respondPluginProtocolError(c, http.StatusBadRequest, "invalid_request", "Invalid image request")
		return nil, true
	}
	keyDigest := sha256.Sum256([]byte(rawKey))
	canonicalBody, err := imageIdempotencyPayload(c, storage, protocolRequest)
	if err != nil {
		respondPluginProtocolError(c, http.StatusBadRequest, "invalid_request", "Invalid image request")
		return nil, true
	}
	bodyDigest := sha256.Sum256(canonicalBody)
	record := &model.TaskIdempotency{
		TokenID:     c.GetInt("token_id"),
		KeyHash:     fmt.Sprintf("%x", keyDigest),
		RequestHash: fmt.Sprintf("%x", bodyDigest),
		TaskID:      model.GenerateTaskID(),
		ExpiresAt:   time.Now().Unix() + 10*60,
	}
	existing, created, err := model.ReserveTaskIdempotency(record, time.Now().Unix())
	if err != nil {
		respondPluginProtocolError(c, http.StatusInternalServerError, "idempotency_error", "Request could not be accepted")
		return nil, true
	}
	if created {
		return record, false
	}
	if existing.RequestHash != record.RequestHash {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": gin.H{
			"message": "Idempotency-Key conflicts with an earlier request",
			"type":    "invalid_request_error",
			"code":    "idempotency_conflict",
		}})
		return nil, true
	}
	if existing.State == model.TaskIdempotencyProcessing {
		c.Header("Retry-After", "3")
		c.AbortWithStatusJSON(http.StatusAccepted, gin.H{"task_id": existing.TaskID, "status": "processing", "message": "图片正在生成中，请使用任务号查询结果", "poll_interval_seconds": 3})
		return nil, true
	}
	if existing.State == model.TaskIdempotencyFailed || existing.TaskID == "" {
		if existing.ErrorCode == "content_rejected" {
			respondTaskPluginImageError(c, service.TaskErrorWrapperLocal(errors.New(service.TaskContentRejectedMessage), "content_rejected", http.StatusBadRequest))
			return nil, true
		}
		respondTaskPluginImageError(c, service.TaskErrorWrapperLocal(errors.New("图片生成失败，请稍后重试"), "image_generation_failed", http.StatusBadGateway))
		return nil, true
	}
	task, exists, loadErr := model.GetByTaskId(c.GetInt("id"), existing.TaskID)
	if loadErr != nil || !exists || task == nil || !apiTokenOwnsTask(c, task) {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": gin.H{"message": "Task not found", "type": "invalid_request_error"}})
		return nil, true
	}
	if task.Status == model.TaskStatusFailure {
		respondTaskPluginImageError(c, service.TaskErrorWrapperLocal(errors.New("图片生成失败，请稍后重试"), "image_generation_failed", http.StatusBadGateway))
		return nil, true
	}
	if task.Status == model.TaskStatusRejected {
		respondTaskPluginImageError(c, service.TaskErrorWrapperLocal(errors.New(service.TaskContentRejectedMessage), "content_rejected", http.StatusBadRequest))
		return nil, true
	}
	writeImageTaskStatus(c, task, task.CreatedAt)
	return nil, true
}

func imageIdempotencyPayload(c *gin.Context, storage common.BodyStorage, request pluginruntime.ProtocolRequestContext) ([]byte, error) {
	bodyValue := request.RequestBody
	if bodyValue == nil {
		bodyValue = request.Body
	}
	canonicalBody, err := common.Marshal(bodyValue)
	if err != nil {
		return nil, err
	}
	contentType, params, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || !strings.EqualFold(contentType, "multipart/form-data") {
		return canonicalBody, nil
	}
	body, err := storage.Bytes()
	if err != nil {
		return nil, err
	}
	reader := multipart.NewReader(bytes.NewReader(body), params["boundary"])
	parts := make([]string, 0)
	for {
		part, nextErr := reader.NextPart()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			return nil, nextErr
		}
		data, readErr := io.ReadAll(io.LimitReader(part, (20<<20)+1))
		_ = part.Close()
		if readErr != nil {
			return nil, readErr
		}
		if len(data) > 20<<20 {
			return nil, errors.New("multipart image request exceeds the allowed size")
		}
		digest := sha256.Sum256(data)
		parts = append(parts, fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%x", part.FormName(), part.FileName(), part.Header.Get("Content-Type"), len(data), digest))
	}
	sort.Strings(parts)
	return common.Marshal(map[string]any{"body": string(canonicalBody), "parts": parts})
}

func completeImageIdempotency(record *model.TaskIdempotency, taskID, errorCode string) {
	if record == nil {
		return
	}
	state := model.TaskIdempotencyAccepted
	if taskID == "" {
		state = model.TaskIdempotencyFailed
	}
	if err := model.DB.Model(&model.TaskIdempotency{}).
		Where("id = ?", record.ID).
		Updates(map[string]any{"task_id": taskID, "state": state, "error_code": errorCode}).Error; err != nil {
		logger.LogError(context.Background(), common.LogText("failed to update image idempotency record: %s", err.Error()))
	}
}

// waitTaskPluginImageTask polls an asynchronous image task inside the client
// request through the ordinary task polling path, so completion settles
// billing exactly as the background poller would, until the task is terminal
// or the protocol timeout elapses. A client disconnect stops waiting; the
// background poller then finishes the durable task.
func waitTaskPluginImageTask(c *gin.Context, task *model.Task, deps pluginProtocolBridgeDeps, remaining time.Duration) *dto.TaskError {
	waitTimeout := min(deps.submissionTimeout, remaining)
	ctx, cancel := context.WithTimeout(c.Request.Context(), waitTimeout)
	defer cancel()
	ctx = service.WithImageTaskInlineDelivery(ctx)
	for {
		timer := time.NewTimer(deps.imagePollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) && c.Request.Context().Err() == nil {
				return service.TaskErrorWrapperLocal(errImageTaskStillProcessing, "task_processing", http.StatusAccepted)
			}
			return service.TaskErrorWrapperLocal(ctx.Err(), "request_cancelled", http.StatusRequestTimeout)
		case <-timer.C:
		}
		if err := deps.pollTask(ctx, task); err != nil {
			logger.LogWarn(c, common.LogText("task protocol image poll failed for task %s: %s", task.TaskID, common.MaskSensitiveInfo(err.Error(), common.UpstreamAddresses(c)...)))
			continue
		}
		if task.Status == model.TaskStatusSuccess || task.Status == model.TaskStatusFailure || task.Status == model.TaskStatusRejected {
			return nil
		}
	}
}

func respondImageTaskAccepted(c *gin.Context, task *model.Task) {
	c.Header("Cache-Control", "private, no-store")
	c.JSON(http.StatusAccepted, gin.H{
		"task_id":               task.TaskID,
		"status":                "processing",
		"message":               "图片正在生成中，请使用任务号查询结果",
		"poll_interval_seconds": 3,
	})
}

// pollTaskPluginImageTask runs one poll round for a single task through the
// same code path as the scheduled task poller, including status CAS and
// completion settlement.
func pollTaskPluginImageTask(ctx context.Context, task *model.Task) error {
	upstreamID := task.GetUpstreamTaskID()
	if upstreamID == "" {
		return errors.New("task has no upstream id")
	}
	return service.UpdateVideoTasks(ctx, task.Platform, map[int][]string{task.ChannelId: {upstreamID}}, map[string]*model.Task{upstreamID: task})
}

func taskPluginImageResponseFormat(request pluginruntime.ProtocolRequestContext) string {
	body, ok := request.Body.(map[string]any)
	if !ok {
		return ""
	}
	switch body["kind"] {
	case string(pluginruntime.BodyJSON):
		value, _ := body["value"].(map[string]any)
		format, _ := value["response_format"].(string)
		return strings.TrimSpace(format)
	case string(pluginruntime.BodyMultipart), string(pluginruntime.BodyForm):
		fields, _ := body["fields"].(map[string][]string)
		if values := fields["response_format"]; len(values) > 0 {
			return strings.TrimSpace(values[0])
		}
	}
	return ""
}

// respondTaskPluginImageError writes an OpenAI error envelope for the Images
// API. DashScope failure bodies arrive as JSON {code, message}; their message
// is surfaced so a client sees the vendor reason instead of raw JSON.
func respondTaskPluginImageError(c *gin.Context, taskErr *dto.TaskError) {
	service.RecordRequestPolicyTermination(c, taskSubmissionAPIError(taskErr))
	status := http.StatusInternalServerError
	if taskErr.StatusCode >= 400 && taskErr.StatusCode <= 599 {
		status = taskErr.StatusCode
	}
	code := taskErr.Code
	message := taskErr.Message
	if status >= http.StatusInternalServerError || !taskErr.LocalError {
		status = http.StatusBadGateway
		code = "service_unavailable"
		message = "当前生成服务繁忙，请稍后重试"
	}
	errorType := "new_api_error"
	if status < http.StatusInternalServerError {
		errorType = "invalid_request_error"
	}
	if requestID := strings.TrimSpace(c.GetString(common.RequestIdKey)); requestID != "" {
		message = common.MessageWithRequestId(message, requestID)
	}
	c.JSON(status, gin.H{"error": gin.H{
		"message": message,
		"type":    errorType,
		"code":    code,
	}})
}
