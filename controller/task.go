package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/middleware"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/relay"
	relaychannel "github.com/QuantumNous/new-api/relay/channel"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/types"
	"github.com/gin-gonic/gin"
)

type taskArtifactResponse struct {
	Key        string `json:"key"`
	Type       string `json:"type"`
	MimeType   string `json:"mime_type,omitempty"`
	ContentURL string `json:"content_url"`
}

var (
	taskArtifactKeyPattern           = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._~-]{0,127}$`)
	errTaskArtifactPluginUnavailable = errors.New("task artifact plugin unavailable")
	errTaskArtifactPlugin            = errors.New("task artifact plugin error")
)

func GetTask(c *gin.Context) {
	startedAt := time.Now()
	defer func() { service.RecordTaskQueryDuration(time.Since(startedAt)) }()
	task, exists, err := model.GetByTaskId(c.GetInt("id"), c.Param("key"))
	if err != nil {
		videoProxyError(c, http.StatusInternalServerError, "server_error", "Failed to query task")
		return
	}
	if !exists || task == nil || !apiTokenOwnsTask(c, task) {
		videoProxyError(c, http.StatusNotFound, "invalid_request_error", "Task not found")
		return
	}
	createdAt := task.CreatedAt
	if createdAt == 0 {
		createdAt = task.SubmitTime
	}
	if task.PrivateData.ImageTask {
		if task.AsyncImage {
			if task.Status == model.TaskStatusSuccess && task.PrivateData.NativeAsync &&
				task.PrivateData.MediaDeliveryStatus == "failed" &&
				(task.PrivateData.ResultExpiresAt == 0 || time.Now().Unix() < task.PrivateData.ResultExpiresAt) {
				deliveryContext, cancel := context.WithTimeout(c.Request.Context(), imageClientWaitTimeout)
				_ = retryNativeAsyncImageDelivery(deliveryContext, task)
				cancel()
			}
			writeImageTaskStatus(c, task, createdAt)
			return
		}
		if task.Status == model.TaskStatusSuccess && task.PrivateData.MediaDeliveryStatus != "succeeded" &&
			(task.PrivateData.ResultExpiresAt == 0 || time.Now().Unix() < task.PrivateData.ResultExpiresAt) {
			deliveryContext, cancel := context.WithTimeout(c.Request.Context(), imageClientWaitTimeout)
			deliveryErr := service.DeliverImageTaskMedia(deliveryContext, task)
			cancel()
			if errors.Is(deliveryErr, service.ErrImageTaskDeliveryInProgress) {
				writeImageTaskStatus(c, task, createdAt)
				return
			}
			if deliveryErr != nil {
				respondImageMediaDeliveryError(c, task.TaskID)
				return
			}
		}
		writeImageTaskStatus(c, task, createdAt)
		return
	}
	failReason := task.FailReason
	if task.Status == model.TaskStatusSuccess && taskFailReasonIsLegacyResultURL(task.FailReason) {
		failReason = ""
	}
	status := string(task.Status)
	resultURL := any(nil)
	message := ""
	if task.Status == model.TaskStatusSuccess {
		if task.PrivateData.ResultExpiresAt > 0 && time.Now().Unix() >= task.PrivateData.ResultExpiresAt {
			status = "expired"
			message = "图片已过期，请重新生成"
		} else if task.PrivateData.ResultExpiresAt > 0 {
			// Do not expose a provider URL. The artifact endpoint is the only
			// customer-facing media address and is capability protected.
			if url, buildErr := service.BuildTaskArtifactContentURL(task.TaskID, "image", task.PrivateData.ResultExpiresAt); buildErr == nil {
				resultURL = url
			}
		}
	}
	if task.Status == model.TaskStatusRejected {
		status = "rejected"
		message = service.TaskContentRejectedMessage
		failReason = service.TaskContentRejectedMessage
	}
	c.JSON(http.StatusOK, gin.H{
		"task_id":     task.TaskID,
		"platform":    task.Platform,
		"status":      status,
		"message":     message,
		"progress":    task.Progress,
		"fail_reason": failReason,
		"result_url":  resultURL,
		"expires_at":  taskExpiresAt(task.PrivateData.ResultExpiresAt),
		"created_at":  createdAt,
		"finished_at": task.FinishTime,
	})
}

func writeImageTaskStatus(c *gin.Context, task *model.Task, createdAt int64) {
	status := "queued"
	message := ""
	resultURL := any(nil)
	switch task.Status {
	case model.TaskStatusNotStart, model.TaskStatusSubmitted, model.TaskStatusQueued:
		status = "queued"
	case model.TaskStatusInProgress:
		status = "processing"
		message = "图片正在生成中，请使用任务号查询结果"
	case model.TaskStatusSuccess:
		status = "succeeded"
		if task.PrivateData.ResultExpiresAt > 0 && time.Now().Unix() >= task.PrivateData.ResultExpiresAt {
			status = "expired"
			message = "图片已过期，请重新生成"
		} else if task.PrivateData.MediaDeliveryStatus == "failed" {
			status = "failed"
			message = "媒体交付失败"
		} else if task.PrivateData.MediaDeliveryStatus != "succeeded" {
			status = "processing"
			message = "图片正在生成中，请使用任务号查询结果"
		} else if task.AsyncImage && task.PrivateData.ResultURL != "" {
			resultURL = task.PrivateData.ResultURL
		} else if task.PrivateData.MediaDeliveryStatus == "succeeded" {
			artifacts, err := projectTaskArtifacts(task)
			if err != nil {
				break
			}
			for _, artifact := range artifacts {
				if artifact.Type != "image" {
					continue
				}
				if url, buildErr := service.BuildTaskArtifactContentURL(task.TaskID, artifact.Key, task.PrivateData.ResultExpiresAt); buildErr == nil {
					resultURL = url
				}
				break
			}
		}
	case model.TaskStatusFailure:
		status = "failed"
		message = task.FailReason
		if message == "" {
			message = "图片生成失败，请稍后重试"
		}
	case model.TaskStatusRejected:
		status = "rejected"
		message = service.TaskContentRejectedMessage
	case model.TaskStatusExpired:
		status = "expired"
		message = "图片已过期，请重新生成"
	}
	response := gin.H{
		"task_id":     task.TaskID,
		"status":      status,
		"message":     message,
		"result_url":  resultURL,
		"expires_at":  taskExpiresAt(task.PrivateData.ResultExpiresAt),
		"created_at":  createdAt,
		"finished_at": task.FinishTime,
	}
	if status == "queued" || status == "processing" {
		response["poll_interval_seconds"] = 3
	}
	c.Header("Cache-Control", "private, no-store")
	c.JSON(http.StatusOK, response)
}

func taskExpiresAt(unixSeconds int64) any {
	if unixSeconds <= 0 {
		return nil
	}
	return time.Unix(unixSeconds, 0).UTC().Format(time.RFC3339)
}

func GetTaskArtifacts(c *gin.Context) {
	task, exists, err := model.GetByTaskId(c.GetInt("id"), c.Param("key"))
	if err != nil {
		writeTaskArtifactError(c, http.StatusInternalServerError, "artifact_internal_error", "Failed to query task")
		return
	}
	if !exists || task == nil || !apiTokenOwnsTask(c, task) {
		writeTaskArtifactError(c, http.StatusNotFound, "artifact_not_found", "Task or artifact not found")
		return
	}
	writeTaskArtifacts(c, task, false)
}

func GetDashboardTaskArtifacts(c *gin.Context) {
	task, exists, err := getTaskForArtifactRequest(c, c.Param("task_id"))
	if err != nil {
		writeTaskArtifactError(c, http.StatusInternalServerError, "artifact_internal_error", "Failed to query task")
		return
	}
	if !exists || task == nil {
		writeTaskArtifactError(c, http.StatusNotFound, "artifact_not_found", "Task or artifact not found")
		return
	}
	writeTaskArtifacts(c, task, true)
}

func writeTaskArtifacts(c *gin.Context, task *model.Task, dashboard bool) {
	c.Header("Cache-Control", "private, no-store")
	artifacts, err := projectTaskArtifacts(task)
	if err != nil {
		writeTaskArtifactProjectionError(c, err)
		return
	}
	items := make([]taskArtifactResponse, 0, len(artifacts))
	for _, artifact := range artifacts {
		contentURL, buildErr := service.BuildTaskArtifactContentURL(task.TaskID, artifact.Key, task.PrivateData.ResultExpiresAt)
		if buildErr != nil {
			writeTaskArtifactError(c, http.StatusInternalServerError, "artifact_url_error", "Failed to build artifact content URL")
			return
		}
		items = append(items, taskArtifactResponse{
			Key:        artifact.Key,
			Type:       artifact.Type,
			MimeType:   artifact.MimeType,
			ContentURL: contentURL,
		})
	}
	response := gin.H{"task_id": task.TaskID, "artifacts": items}
	if dashboard && task.Status == model.TaskStatusSuccess && task.Platform == constant.TaskPlatformSuno {
		response["legacy_audio_clips"] = legacySunoAudioClips(task.Data)
	}
	if legacyVideoAvailable(task) {
		legacyContentURL, buildErr := service.BuildTaskArtifactContentURL(task.TaskID, "video")
		if buildErr != nil {
			writeTaskArtifactError(c, http.StatusInternalServerError, "artifact_url_error", "Failed to build artifact content URL")
			return
		}
		response["legacy_content_url"] = legacyContentURL
	}
	if dashboard {
		common.ApiSuccess(c, response)
		return
	}
	c.JSON(http.StatusOK, response)
}

// legacySunoAudioClips projects a pre-plugin Suno task's persisted snapshot
// into the clips the dashboard audio preview renders. Task lists no longer
// carry the snapshot, so the dashboard reads it here on demand. The snapshot
// is either a clip array or a JSON string holding one; only clips with an
// audio URL are kept and only preview fields are exposed.
func legacySunoAudioClips(data json.RawMessage) []map[string]any {
	clips := make([]map[string]any, 0)
	if len(data) == 0 {
		return clips
	}
	var items []map[string]any
	if err := common.Unmarshal(data, &items); err != nil {
		var encoded string
		if common.Unmarshal(data, &encoded) != nil || common.UnmarshalJsonStr(encoded, &items) != nil {
			return clips
		}
	}
	for _, item := range items {
		audioURL, _ := item["audio_url"].(string)
		if strings.TrimSpace(audioURL) == "" {
			continue
		}
		clip := map[string]any{"audio_url": audioURL}
		for _, key := range []string{"clip_id", "id", "title", "tags", "duration", "image_url", "image_large_url", "metadata"} {
			if value, ok := item[key]; ok {
				clip[key] = value
			}
		}
		clips = append(clips, clip)
	}
	return clips
}

func projectTaskArtifacts(task *model.Task) ([]relaychannel.TaskArtifact, error) {
	if task == nil || task.Status != model.TaskStatusSuccess || !taskHasPluginExecution(task) || !task.ResultRetrievable() {
		return []relaychannel.TaskArtifact{}, nil
	}
	adaptor := relay.GetTaskAdaptor(task.Platform)
	if adaptor == nil {
		return nil, errTaskArtifactPluginUnavailable
	}
	provider, ok := adaptor.(relaychannel.TaskArtifactProvider)
	if !ok {
		return []relaychannel.TaskArtifact{}, nil
	}
	artifacts, err := provider.ListArtifacts(task)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errTaskArtifactPlugin, err)
	}
	return validateProjectedTaskArtifacts(artifacts)
}

func validateProjectedTaskArtifacts(artifacts []relaychannel.TaskArtifact) ([]relaychannel.TaskArtifact, error) {
	if len(artifacts) > 64 {
		return nil, fmt.Errorf("%w: too many artifacts", errTaskArtifactPlugin)
	}
	seen := make(map[string]struct{}, len(artifacts))
	for i := range artifacts {
		if artifacts[i].Key != strings.TrimSpace(artifacts[i].Key) ||
			artifacts[i].Type != strings.TrimSpace(artifacts[i].Type) {
			return nil, fmt.Errorf("%w: invalid artifact identity", errTaskArtifactPlugin)
		}
		if !taskArtifactKeyPattern.MatchString(artifacts[i].Key) {
			return nil, fmt.Errorf("%w: invalid artifact key", errTaskArtifactPlugin)
		}
		if _, exists := seen[artifacts[i].Key]; exists {
			return nil, fmt.Errorf("%w: duplicate artifact key", errTaskArtifactPlugin)
		}
		seen[artifacts[i].Key] = struct{}{}
		switch artifacts[i].Type {
		case "video", "audio", "image", "file":
		default:
			return nil, fmt.Errorf("%w: invalid artifact type", errTaskArtifactPlugin)
		}
		if len(artifacts[i].MimeType) > 255 || strings.ContainsAny(artifacts[i].MimeType, "\r\n") {
			return nil, fmt.Errorf("%w: invalid artifact mime type", errTaskArtifactPlugin)
		}
	}
	return artifacts, nil
}

func initTaskArtifactAdaptor(task *model.Task) (relaychannel.TaskAdaptor, error) {
	if task == nil || !taskHasPluginExecution(task) {
		return nil, errTaskArtifactPluginUnavailable
	}
	channelModel, err := model.CacheGetChannel(task.ChannelId)
	if err != nil {
		return nil, fmt.Errorf("%w: channel unavailable", errTaskArtifactPluginUnavailable)
	}
	adaptor := relay.GetTaskAdaptor(task.Platform)
	if adaptor == nil {
		return nil, errTaskArtifactPluginUnavailable
	}
	pluginKey := task.PrivateData.Key
	if pluginKey == "" {
		pluginKey = channelModel.Key
	}
	baseURL := channelModel.GetBaseURL()
	if baseURL == "" {
		baseURL = constant.GetChannelBaseURL(channelModel.Type)
	}
	adaptor.Init(&relaycommon.RelayInfo{
		ChannelMeta: &relaycommon.ChannelMeta{
			ChannelType:     channelModel.Type,
			ChannelBaseUrl:  baseURL,
			ApiKey:          pluginKey,
			ChannelSetting:  channelModel.GetSetting(),
			HeadersOverride: channelModel.GetHeaderOverride(),
		},
	})
	return adaptor, nil
}

func taskHasPluginExecution(task *model.Task) bool {
	return task != nil &&
		task.PrivateData.Execution != nil &&
		task.PrivateData.Execution.TaskPlugin != nil &&
		strings.TrimSpace(task.PrivateData.Execution.TaskPlugin.Key) != ""
}

func legacyVideoAvailable(task *model.Task) bool {
	if task == nil || task.Status != model.TaskStatusSuccess ||
		taskHasPluginExecution(task) || task.Platform == constant.TaskPlatformSuno ||
		strings.TrimSpace(task.GetResultURL()) == "" {
		return false
	}
	switch constant.NormalizeTaskAction(task.Action) {
	case constant.TaskActionImageToVideo,
		constant.TaskActionTextToVideo,
		constant.TaskActionFirstTailToVideo,
		constant.TaskActionReferenceToVideo,
		constant.TaskActionRemix:
		return true
	default:
		return false
	}
}

func getTaskForArtifactRequest(c *gin.Context, taskID string) (*model.Task, bool, error) {
	if middleware.IsTaskArtifactAccess(c) {
		task, exists, err := model.GetUniqueByOnlyTaskId(taskID)
		if err != nil || !exists || task == nil {
			return task, exists, err
		}
		owner, err := model.GetUserCache(task.UserId)
		if err != nil || owner == nil || owner.Status != common.UserStatusEnabled {
			return nil, false, err
		}
		return task, true, nil
	}
	if c.GetInt("token_id") == 0 && c.GetInt("role") >= common.RoleAdminUser {
		return model.GetByOnlyTaskId(taskID)
	}
	task, exists, err := model.GetByTaskId(c.GetInt("id"), taskID)
	if err != nil || !exists || task == nil || !apiTokenOwnsTask(c, task) {
		return task, exists && apiTokenOwnsTask(c, task), err
	}
	return task, true, nil
}

// apiTokenOwnsTask supplements user ownership with the API key identity. A
// task without a recorded token ID is not exposed through a token-authenticated
// customer route because its key-level owner cannot be established.
func apiTokenOwnsTask(c *gin.Context, task *model.Task) bool {
	tokenID := c.GetInt("token_id")
	return tokenID <= 0 || task != nil && task.PrivateData.TokenId == tokenID
}

func writeTaskArtifactProjectionError(c *gin.Context, err error) {
	if errors.Is(err, errTaskArtifactPluginUnavailable) {
		writeTaskArtifactError(c, http.StatusServiceUnavailable, "artifact_plugin_unavailable", "Artifact preview plugin is unavailable")
		return
	}
	writeTaskArtifactError(c, http.StatusInternalServerError, "artifact_plugin_error", "Artifact preview plugin failed")
}

func writeTaskArtifactError(c *gin.Context, status int, code, message string) {
	c.Header("Cache-Control", "private, no-store")
	if middleware.IsTaskArtifactAccess(c) {
		status = http.StatusNotFound
		code = "artifact_not_found"
		message = "Task or artifact not found"
	}
	if strings.HasPrefix(c.Request.URL.Path, "/api/") {
		c.JSON(status, gin.H{"success": false, "code": code, "message": message})
		return
	}
	c.JSON(status, gin.H{
		"error": gin.H{
			"message": message,
			"type":    code,
			"code":    code,
		},
	})
}

func TaskArtifactContent(c *gin.Context) {
	task, exists, err := getTaskForArtifactRequest(c, c.Param("key"))
	if err != nil {
		writeTaskArtifactError(c, http.StatusInternalServerError, "artifact_internal_error", "Failed to query task")
		return
	}
	if !exists || task == nil {
		writeTaskArtifactError(c, http.StatusNotFound, "artifact_not_found", "Task or artifact not found")
		return
	}
	artifactKey := strings.TrimSpace(c.Param("artifact_key"))
	if !taskArtifactKeyPattern.MatchString(artifactKey) || !task.ResultRetrievable() {
		writeTaskArtifactError(c, http.StatusNotFound, "artifact_not_found", "Task or artifact not found")
		return
	}
	if task.Status != model.TaskStatusSuccess {
		if task.Status == model.TaskStatusExpired {
			writeTaskArtifactError(c, http.StatusGone, "artifact_expired", "图片已过期，请重新生成")
			return
		}
		writeTaskArtifactError(c, http.StatusConflict, "artifact_not_ready", "Task artifacts are not ready")
		return
	}
	if task.PrivateData.ImageTask && task.PrivateData.ResultExpiresAt > 0 && time.Now().Unix() >= task.PrivateData.ResultExpiresAt {
		if err := service.GetTaskArtifactStore().Delete(task); err != nil {
			logger.LogError(c.Request.Context(), common.LogText("expired image cleanup failed for task %s: %v", task.TaskID, err))
		}
		if _, err := model.MarkImageTaskExpired(task.TaskID); err != nil {
			logger.LogError(c.Request.Context(), common.LogText("mark image task expired failed for task %s: %v", task.TaskID, err))
		}
		writeTaskArtifactError(c, http.StatusGone, "artifact_expired", "图片已过期，请重新生成")
		return
	}
	artifactStore := service.GetTaskArtifactStore()
	if task.PrivateData.ImageTask && !artifactStore.Enabled() {
		writeTaskArtifactError(c, http.StatusBadGateway, "media_delivery_failed", "媒体交付失败")
		return
	}
	if !taskHasPluginExecution(task) {
		if artifactKey != "video" || !legacyVideoAvailable(task) {
			writeTaskArtifactError(c, http.StatusNotFound, "artifact_not_found", "Task or artifact not found")
			return
		}
		descriptor := &relaychannel.TaskContentRequest{
			URL:            task.GetResultURL(),
			Method:         c.Request.Method,
			Credentialless: true,
		}
		if err := proxyTaskMedia(c, task, descriptor); err != nil {
			writeTaskMediaProxyError(c, err)
		}
		return
	}
	// A signed capability has already bound the request to this task and key.
	// Serve a persisted artifact before running the plugin projection, which can
	// execute JavaScript and re-parse the upstream task snapshot on every GET.
	if middleware.IsTaskArtifactAccess(c) {
		if ref, resolveErr := artifactStore.Resolve(task, artifactKey); resolveErr == nil && ref != nil {
			if serveErr := artifactStore.Serve(c, task, ref); serveErr == nil {
				return
			}
		}
	}
	c.Set("task_artifact_cache_key", artifactKey)
	artifacts, err := projectTaskArtifacts(task)
	if err != nil {
		writeTaskArtifactProjectionError(c, err)
		return
	}
	found := false
	for _, artifact := range artifacts {
		if artifact.Key == artifactKey {
			found = true
			break
		}
	}
	if !found {
		writeTaskArtifactError(c, http.StatusNotFound, "artifact_not_found", "Task or artifact not found")
		return
	}
	if ref, resolveErr := artifactStore.Resolve(task, artifactKey); resolveErr == nil && ref != nil {
		_ = artifactStore.Serve(c, task, ref)
		return
	}

	adaptor, err := initTaskArtifactAdaptor(task)
	if err != nil {
		writeTaskArtifactProjectionError(c, err)
		return
	}
	provider, ok := adaptor.(relaychannel.TaskContentRequestProvider)
	if !ok {
		writeTaskArtifactError(c, http.StatusServiceUnavailable, "artifact_plugin_unavailable", "Artifact content plugin is unavailable")
		return
	}
	clientRequest := relaychannel.TaskArtifactClientRequest{
		Method:  c.Request.Method,
		Headers: taskArtifactClientHeaders(c.Request.Header),
	}
	descriptor, err := provider.BuildContentRequest(c.Request.Context(), task, artifactKey, clientRequest)
	if err != nil || descriptor == nil {
		if err != nil {
			logger.LogWarn(c.Request.Context(), fmt.Sprintf("Failed to resolve plugin %s content for task %s: %v", artifactKey, task.TaskID, err))
		}
		writeTaskArtifactError(c, http.StatusInternalServerError, "artifact_plugin_error", "Artifact content plugin failed")
		return
	}
	if err := proxyTaskMedia(c, task, descriptor); err != nil {
		if task.PrivateData.ImageTask {
			logger.LogWarn(c.Request.Context(), common.LogText("image media delivery failed for task %s", task.TaskID))
			service.MarkImageDeliveryFailed(task)
			writeTaskArtifactError(c, http.StatusBadGateway, "media_delivery_failed", "媒体交付失败")
			return
		}
		writeTaskMediaProxyError(c, err)
	}
}

func taskArtifactClientHeaders(headers http.Header) map[string]string {
	result := make(map[string]string, 4)
	for _, name := range []string{"Range", "If-Range", "If-None-Match", "If-Modified-Since"} {
		if value := strings.TrimSpace(headers.Get(name)); value != "" {
			result[name] = value
		}
	}
	return result
}

/*
	The task list handlers below deliberately do not call projectTaskArtifacts.
	Artifact projection is confined to the explicit endpoints above.
*/

func GetAllTask(c *gin.Context) {
	pageInfo := common.GetPageQuery(c)
	startTimestamp, _ := strconv.ParseInt(c.Query("start_timestamp"), 10, 64)
	endTimestamp, _ := strconv.ParseInt(c.Query("end_timestamp"), 10, 64)
	queryParams := model.SyncTaskQueryParams{Platform: constant.TaskPlatform(c.Query("platform")), TaskID: c.Query("task_id"), Status: c.Query("status"), Action: c.Query("action"), StartTimestamp: startTimestamp, EndTimestamp: endTimestamp, ChannelID: c.Query("channel_id")}
	items := model.TaskGetAllTasks(pageInfo.GetStartIdx(), pageInfo.GetPageSize(), queryParams)
	pageInfo.SetTotal(int(model.TaskCountAllTasks(queryParams)))
	pageInfo.SetItems(tasksToDto(items, true, c.GetInt("role")))
	common.ApiSuccess(c, pageInfo)
}

func GetUserTask(c *gin.Context) {
	pageInfo := common.GetPageQuery(c)
	userID := c.GetInt("id")
	startTimestamp, _ := strconv.ParseInt(c.Query("start_timestamp"), 10, 64)
	endTimestamp, _ := strconv.ParseInt(c.Query("end_timestamp"), 10, 64)
	queryParams := model.SyncTaskQueryParams{Platform: constant.TaskPlatform(c.Query("platform")), TaskID: c.Query("task_id"), Status: c.Query("status"), Action: c.Query("action"), StartTimestamp: startTimestamp, EndTimestamp: endTimestamp}
	items := model.TaskGetAllUserTask(userID, pageInfo.GetStartIdx(), pageInfo.GetPageSize(), queryParams)
	pageInfo.SetTotal(int(model.TaskCountAllUserTask(userID, queryParams)))
	pageInfo.SetItems(tasksToDto(items, false, common.RoleCommonUser))
	common.ApiSuccess(c, pageInfo)
}

func tasksToDto(tasks []*model.Task, fillUser bool, viewerRole int) []*dto.TaskDto {
	var userIDMap map[int]*model.UserBase
	if fillUser {
		userIDMap = make(map[int]*model.UserBase)
		userIDs := types.NewSet[int]()
		for _, task := range tasks {
			userIDs.Add(task.UserId)
		}
		for _, userID := range userIDs.Items() {
			if cacheUser, err := model.GetUserCache(userID); err == nil {
				userIDMap[userID] = cacheUser
			}
		}
	}
	result := make([]*dto.TaskDto, len(tasks))
	for i, task := range tasks {
		if fillUser {
			if user, ok := userIDMap[task.UserId]; ok {
				task.Username = user.Username
			}
		}
		item := relay.TaskModel2Dto(task)
		item.LegacyVideoAvailable = legacyVideoAvailable(task)
		item.ResultDiscarded = task.PrivateData.ResultDiscarded
		if task.PrivateData.ImageTask {
			item.ResultURL = ""
			if task.Status == model.TaskStatusFailure {
				item.FailReason = "图片生成失败，请稍后重试"
			}
			if task.Status == model.TaskStatusRejected {
				item.FailReason = service.TaskContentRejectedMessage
			}
			if viewerRole < common.RoleAdminUser {
				item.Data = nil
			}
		}
		if task.Status == model.TaskStatusRejected && viewerRole < common.RoleAdminUser {
			item.FailReason = service.TaskContentRejectedMessage
			item.Data = nil
		}
		if task.Status == model.TaskStatusSuccess {
			item.ResultURL = ""
			if taskFailReasonIsLegacyResultURL(task.FailReason) {
				item.FailReason = ""
			}
		}
		if viewerRole >= common.RoleAdminUser {
			adminInfo := &dto.TaskAdminInfo{}
			if execution := task.PrivateData.Execution; execution != nil {
				adminInfo.RequestID = execution.RequestID
				adminInfo.RequestPath = execution.RequestPath
				if snapshot := execution.TaskPlugin; snapshot != nil {
					adminInfo.TaskPlugin = &dto.TaskPluginInfo{
						Key:     snapshot.Key,
						Name:    snapshot.Name,
						Version: snapshot.Version,
					}
					if snapshot.Author != nil {
						adminInfo.TaskPlugin.Author = &dto.TaskPluginAuthorInfo{
							Name: snapshot.Author.Name,
							URL:  snapshot.Author.URL,
						}
					}
				}
			}
			if adminInfo.RequestID != "" || adminInfo.RequestPath != "" || adminInfo.TaskPlugin != nil {
				item.AdminInfo = adminInfo
			}
		}
		if viewerRole >= common.RoleRootUser {
			rootInfo := &dto.TaskRootInfo{
				UpstreamTaskID: task.PrivateData.UpstreamTaskID,
				NodeName:       task.PrivateData.NodeName,
			}
			if execution := task.PrivateData.Execution; execution != nil {
				if snapshot := execution.TaskPlugin; snapshot != nil {
					rootInfo.TaskPlugin = &dto.TaskPluginRuntimeInfo{
						Key:        snapshot.Key,
						Version:    snapshot.Version,
						APIVersion: snapshot.APIVersion,
						Generation: snapshot.Generation,
					}
				}
			}
			if rootInfo.TaskPlugin != nil || rootInfo.UpstreamTaskID != "" || rootInfo.NodeName != "" {
				item.RootInfo = rootInfo
			}
		}
		result[i] = item
	}
	return result
}

func taskFailReasonIsLegacyResultURL(value string) bool {
	value = strings.TrimSpace(value)
	return len(value) >= len("https://") && strings.EqualFold(value[:len("https://")], "https://") ||
		len(value) >= len("http://") && strings.EqualFold(value[:len("http://")], "http://") ||
		len(value) >= len("data:") && strings.EqualFold(value[:len("data:")], "data:")
}
