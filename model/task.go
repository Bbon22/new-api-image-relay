package model

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/pkg/billingexpr"
	commonRelay "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/dto"
)

type TaskStatus string

func (t TaskStatus) ToVideoStatus() string {
	var status string
	switch t {
	case TaskStatusNotStart, TaskStatusQueued, TaskStatusSubmitted:
		status = dto.VideoStatusQueued
	case TaskStatusInProgress:
		status = dto.VideoStatusInProgress
	case TaskStatusSuccess:
		status = dto.VideoStatusCompleted
	case TaskStatusFailure:
		status = dto.VideoStatusFailed
	case TaskStatusRejected:
		status = dto.VideoStatusFailed
	default:
		status = dto.VideoStatusUnknown // Default fallback
	}
	return status
}

const (
	TaskStatusNotStart   TaskStatus = "NOT_START"
	TaskStatusSubmitted             = "SUBMITTED"
	TaskStatusQueued                = "QUEUED"
	TaskStatusInProgress            = "IN_PROGRESS"
	TaskStatusFailure               = "FAILURE"
	TaskStatusSuccess               = "SUCCESS"
	TaskStatusRejected              = "REJECTED"
	// EXPIRED means a previously successful hosted result is no longer retained.
	// It is not a billing failure and must never trigger a refund.
	TaskStatusExpired = "EXPIRED"
	TaskStatusUnknown = "UNKNOWN"
)

// TaskResultRetentionSeconds is the customer-visible hosted result window.
// Task rows outlive this window; cleanup changes only media availability.
const TaskResultRetentionSeconds int64 = 9 * 60 * 60

// TaskRefundLegacyCutoff separates tasks created before timeout refunds were
// introduced. Those legacy tasks are failed without an automatic refund.
const TaskRefundLegacyCutoff int64 = 1771718400 // 2026-02-22 00:00:00 UTC

type Task struct {
	ID        int64                 `json:"id" gorm:"primary_key;AUTO_INCREMENT"`
	CreatedAt int64                 `json:"created_at" gorm:"index"`
	UpdatedAt int64                 `json:"updated_at"`
	TaskID    string                `json:"task_id" gorm:"type:varchar(191);index"` // 第三方id，不一定有/ song id\ Task id
	Platform  constant.TaskPlatform `json:"platform" gorm:"type:varchar(30);index"` // 平台
	UserId    int                   `json:"user_id" gorm:"index"`
	Group     string                `json:"group" gorm:"type:varchar(50)"` // 修正计费用
	ChannelId int                   `json:"channel_id" gorm:"index"`
	Quota     int                   `json:"quota"`
	Action    string                `json:"action" gorm:"type:varchar(40);index"` // 任务类型, song, lyrics, description-mode
	Status    TaskStatus            `json:"status" gorm:"type:varchar(20);index"` // 任务状态
	ImageTask bool                  `json:"-" gorm:"index"`
	// AsyncImage marks durable background image submissions.
	AsyncImage bool       `json:"-" gorm:"index"`
	FailReason string     `json:"fail_reason"`
	SubmitTime int64      `json:"submit_time" gorm:"index"`
	StartTime  int64      `json:"start_time" gorm:"index"`
	FinishTime int64      `json:"finish_time" gorm:"index"`
	RefundedAt int64      `json:"refunded_at,omitempty" gorm:"index"`
	Progress   string     `json:"progress" gorm:"type:varchar(20);index"`
	Properties Properties `json:"properties" gorm:"type:json"`
	Username   string     `json:"username,omitempty" gorm:"-"`
	// 禁止返回给用户，内部可能包含key等隐私信息
	PrivateData TaskPrivateData `json:"-" gorm:"column:private_data;type:json"`
	Data        json.RawMessage `json:"data" gorm:"type:json"`
}

func (t *Task) SetData(data any) {
	b, _ := common.Marshal(data)
	t.Data = json.RawMessage(b)
}

func (t *Task) GetData(v any) error {
	return common.Unmarshal(t.Data, &v)
}

type Properties struct {
	Input             string `json:"input"`
	UpstreamModelName string `json:"upstream_model_name,omitempty"`
	OriginModelName   string `json:"origin_model_name,omitempty"`
}

func (m *Properties) Scan(val any) error {
	bytesValue := jsonScanBytes(val)
	if len(bytesValue) == 0 {
		*m = Properties{}
		return nil
	}
	return common.Unmarshal(bytesValue, m)
}

func (m Properties) Value() (driver.Value, error) {
	if m == (Properties{}) {
		return nil, nil
	}
	// 必须返回 string 而非 []byte:PG simple protocol 下 []byte 按 bytea 编码,
	// 写 json 列会触发 SQLSTATE 22P02。
	b, err := common.Marshal(m)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

type TaskPrivateData struct {
	Key             string   `json:"key,omitempty"`
	UpstreamTaskID  string   `json:"upstream_task_id,omitempty"`  // 上游真实 task ID
	ResultURL       string   `json:"result_url,omitempty"`        // 任务成功后的结果 URL（视频地址等）
	ResultExpiresAt int64    `json:"result_expires_at,omitempty"` // 平台托管结果过期时间（Unix seconds）
	ImageTask       bool     `json:"image_task,omitempty"`
	HostedImageKeys []string `json:"hosted_image_keys,omitempty"`
	// SettlementStatus is independent from the task lifecycle. A successful
	// task remains financially settled after it transitions to EXPIRED.
	SettlementStatus string `json:"settlement_status,omitempty"`
	// FailureClass is internal telemetry only and is never exposed to clients.
	FailureClass string `json:"failure_class,omitempty"`
	// MediaDeliveryStatus tracks the platform's image hosting step separately
	// from upstream generation and financial settlement.
	MediaDeliveryStatus string `json:"media_delivery_status,omitempty"`
	// Execution records safe, immutable request provenance. It lives next to
	// other private task state so public task DTOs cannot expose it by accident.
	Execution *TaskExecutionSnapshot `json:"execution,omitempty"`
	// 计费上下文：用于异步退款/差额结算（轮询阶段读取）
	BillingSource  string              `json:"billing_source,omitempty"`  // "wallet" 或 "subscription"
	SubscriptionId int                 `json:"subscription_id,omitempty"` // 订阅 ID，用于订阅退款
	TokenId        int                 `json:"token_id,omitempty"`        // 令牌 ID，用于令牌额度退款
	NodeName       string              `json:"node_name,omitempty"`       // 发起任务的节点名，轮询结算阶段据此归属日志而非最后查询节点
	BillingContext *TaskBillingContext `json:"billing_context,omitempty"` // 计费参数快照（用于轮询阶段重新计算）
	// ResponsesBackground records that the openai_responses create request
	// asked for background:true. Every task is durable and survives client
	// disconnect regardless; this only echoes the protocol-level request
	// attribute back on retrieval snapshots.
	ResponsesBackground bool `json:"responses_background,omitempty"`
	// PluginState is plugin-owned cross-round data. Unlike Task.Data it is
	// only replaced when a hook explicitly returns state.
	PluginState json.RawMessage `json:"plugin_state,omitempty"`
	// PollFailures counts consecutive unrecognized or transient poll outcomes.
	PollFailures int `json:"poll_failures,omitempty"`
	// ResultDiscarded marks an immediate terminal result whose submit route
	// declared retainResult: false. The upstream snapshot was never written
	// and every retrieval surface treats the task as not found. The zero
	// value keeps historical rows retained and retrievable.
	ResultDiscarded bool `json:"result_discarded,omitempty"`
	// Async image request input is private and only used to recover queued work
	// after a process restart. []byte is base64 encoded by the JSON serializer.
	AsyncRequestPath        string `json:"async_request_path,omitempty"`
	AsyncRequestContentType string `json:"async_request_content_type,omitempty"`
	AsyncRequestBody        []byte `json:"async_request_body,omitempty"`
	AsyncTokenGroup         string `json:"async_token_group,omitempty"`
	AsyncTokenUnlimited     bool   `json:"async_token_unlimited,omitempty"`
	NativeAsync             bool   `json:"native_async,omitempty"`
}

const (
	TaskFailureClassUpstream     = "upstream"
	TaskFailureClassLocal        = "local"
	TaskFailureClassUnclassified = "unclassified"
)

// TaskHealthStats is an internal, terminal-task view used by operations
// monitoring and the daily report. Rejected prompts are reported separately
// and excluded from service success-rate calculations. Expired image tasks
// count as successful because delivery completed before media retention elapsed.
type TaskHealthStats struct {
	Ended            int64
	Succeeded        int64
	Failed           int64
	Rejected         int64
	UpstreamFailures int64
	LocalFailures    int64
	Unclassified     int64
}

func GetImageTaskHealthStats(since, until int64) (TaskHealthStats, error) {
	var stats TaskHealthStats
	if until <= 0 {
		until = time.Now().Unix()
	}
	if since < 0 {
		since = 0
	}
	var tasks []*Task
	err := DB.Where("image_task = ? AND finish_time >= ? AND finish_time <= ? AND status IN ?", true, since, until,
		[]string{string(TaskStatusSuccess), string(TaskStatusExpired), string(TaskStatusFailure), string(TaskStatusRejected)}).
		Find(&tasks).Error
	if err != nil {
		return stats, err
	}
	for _, task := range tasks {
		if task == nil {
			continue
		}
		if task.Status == TaskStatusRejected {
			stats.Rejected++
			continue
		}
		stats.Ended++
		if task.PrivateData.MediaDeliveryStatus == "failed" {
			stats.Failed++
			stats.LocalFailures++
			continue
		}
		switch task.Status {
		case TaskStatusSuccess, TaskStatusExpired:
			stats.Succeeded++
		case TaskStatusFailure:
			stats.Failed++
			switch task.PrivateData.FailureClass {
			case TaskFailureClassUpstream:
				stats.UpstreamFailures++
			case TaskFailureClassLocal:
				stats.LocalFailures++
			default:
				stats.Unclassified++
			}
		}
	}
	return stats, nil
}

type TaskExecutionSnapshot struct {
	RequestID   string              `json:"request_id,omitempty"`
	RequestPath string              `json:"request_path,omitempty"`
	TaskPlugin  *TaskPluginSnapshot `json:"task_plugin,omitempty"`
}

// TaskPluginSnapshot contains credential-free identity only. Plugin source,
// request/response payloads, and channel secrets must never be added here.
type TaskPluginSnapshot struct {
	Key        string                    `json:"key"`
	Name       string                    `json:"name"`
	Version    string                    `json:"version"`
	Author     *TaskPluginAuthorSnapshot `json:"author,omitempty"`
	APIVersion int                       `json:"api_version"`
	Generation uint64                    `json:"generation"`
}

type TaskPluginAuthorSnapshot struct {
	Name string `json:"name"`
	URL  string `json:"url,omitempty"`
}

// TaskBillingContext 记录任务提交时的计费参数，以便轮询阶段可以重新计算额度。
type TaskBillingContext struct {
	ModelPrice      float64                      `json:"model_price,omitempty"`       // 模型单价
	GroupRatio      float64                      `json:"group_ratio,omitempty"`       // 分组倍率
	ModelRatio      float64                      `json:"model_ratio,omitempty"`       // 模型倍率
	OtherRatios     map[string]float64           `json:"other_ratios,omitempty"`      // 附加倍率（时长、分辨率等）
	OriginModelName string                       `json:"origin_model_name,omitempty"` // 模型名称，必须为OriginModelName
	PerCallBilling  bool                         `json:"per_call_billing,omitempty"`  // 按次计费：跳过轮询阶段的差额结算
	TieredSnapshot  *billingexpr.BillingSnapshot `json:"tiered_snapshot,omitempty"`
}

// ResultRetrievable reports whether retrieval surfaces (native query routes,
// protocol retrieve endpoints, artifact projection) may serve this task.
func (t *Task) ResultRetrievable() bool {
	return !t.PrivateData.ResultDiscarded
}

// GetImageTasksForArtifactCleanup returns image task rows in stable ID order.
// The caller filters by the recorded result expiry and removes media only;
// task rows remain available for status and settlement history.
func GetImageTasksForArtifactCleanup(afterID, finishedBefore int64, limit int) ([]*Task, error) {
	if limit <= 0 {
		limit = 100
	}
	var tasks []*Task
	err := DB.Where("id > ? AND image_task = ? AND status IN ? AND finish_time <= ?", afterID, true, []string{string(TaskStatusSuccess), string(TaskStatusExpired)}, finishedBefore).
		Order("id").Limit(limit).Find(&tasks).Error
	return tasks, err
}

// GetImageTasksNeedingDelivery returns recent successful image tasks in stable
// ID order. Callers filter the private delivery state and enqueue only work
// interrupted before it could be marked failed or succeeded.
func GetImageTasksNeedingDelivery(afterID, finishedAfter int64, limit int) ([]*Task, error) {
	if limit <= 0 {
		limit = 100
	}
	var tasks []*Task
	err := DB.Where("id > ? AND image_task = ? AND status = ? AND finish_time >= ?", afterID, true, TaskStatusSuccess, finishedAfter).
		Order("id").Limit(limit).Find(&tasks).Error
	return tasks, err
}

// GetOldestSuccessfulImageTasks pages successful images in completion order so
// disk-pressure eviction consistently expires the oldest customer results first.
func GetOldestSuccessfulImageTasks(afterFinishTime, afterID, finishedBefore int64, limit int) ([]*Task, error) {
	if limit <= 0 {
		limit = 100
	}
	query := DB.Where("image_task = ? AND status = ? AND finish_time <= ?", true, TaskStatusSuccess, finishedBefore)
	if afterFinishTime > 0 || afterID > 0 {
		query = query.Where("finish_time > ? OR (finish_time = ? AND id > ?)", afterFinishTime, afterFinishTime, afterID)
	}
	var tasks []*Task
	err := query.Order("finish_time").Order("id").Limit(limit).Find(&tasks).Error
	return tasks, err
}

// DeleteExpiredTaskHistory removes at most limit task rows. Successfully
// refunded tasks remain for seven days; all other rows remain for three.
func DeleteExpiredTaskHistory(now int64, limit int) (int64, error) {
	if limit <= 0 {
		limit = 500
	}
	threeDaysAgo := now - 3*24*60*60
	sevenDaysAgo := now - 7*24*60*60
	var ids []int64
	if err := DB.Model(&Task{}).
		Where("created_at < ? AND (refunded_at = 0 OR created_at < ?)", threeDaysAgo, sevenDaysAgo).
		Order("id").Limit(limit).Pluck("id", &ids).Error; err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	result := DB.Where("id IN ?", ids).Delete(&Task{})
	return result.RowsAffected, result.Error
}

func MarkTaskRefunded(taskID string, refundedAt int64) error {
	return DB.Model(&Task{}).
		Where("task_id = ? AND refunded_at = 0", taskID).
		Update("refunded_at", refundedAt).Error
}

// GetUpstreamTaskID 获取上游真实 task ID（用于与 provider 通信）
// 旧数据没有 UpstreamTaskID 时，TaskID 本身就是上游 ID
func (t *Task) GetUpstreamTaskID() string {
	if t.PrivateData.UpstreamTaskID != "" {
		return t.PrivateData.UpstreamTaskID
	}
	return t.TaskID
}

// GetResultURL 获取任务结果 URL（视频地址等）
// 新数据存在 PrivateData.ResultURL 中；旧数据回退到 FailReason（历史兼容）
func (t *Task) GetResultURL() string {
	if t.PrivateData.ResultURL != "" {
		return t.PrivateData.ResultURL
	}
	return t.FailReason
}

// GenerateTaskID 生成对外暴露的 task_xxxx 格式 ID
func GenerateTaskID() string {
	key, _ := common.GenerateRandomCharsKey(32)
	return "task_" + key
}

func (p *TaskPrivateData) Scan(val any) error {
	bytesValue := jsonScanBytes(val)
	if len(bytesValue) == 0 {
		return nil
	}
	return common.Unmarshal(bytesValue, p)
}

func (p TaskPrivateData) Value() (driver.Value, error) {
	if p.Key == "" && p.UpstreamTaskID == "" && p.ResultURL == "" &&
		p.ResultExpiresAt == 0 && p.SettlementStatus == "" && p.FailureClass == "" && p.MediaDeliveryStatus == "" && !p.ImageTask && len(p.HostedImageKeys) == 0 && p.Execution == nil && p.BillingSource == "" && p.SubscriptionId == 0 &&
		p.TokenId == 0 && p.NodeName == "" && p.BillingContext == nil &&
		!p.ResponsesBackground && len(p.PluginState) == 0 && p.PollFailures == 0 &&
		!p.ResultDiscarded && p.AsyncRequestPath == "" && p.AsyncRequestContentType == "" &&
		len(p.AsyncRequestBody) == 0 && p.AsyncTokenGroup == "" && !p.AsyncTokenUnlimited && !p.NativeAsync {
		return nil, nil
	}
	// 同 Properties.Value:string 避免 PG simple protocol 的 bytea 编码。
	b, err := common.Marshal(p)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

// SyncTaskQueryParams 用于包含所有搜索条件的结构体，可以根据需求添加更多字段
type SyncTaskQueryParams struct {
	Platform       constant.TaskPlatform
	ChannelID      string
	TaskID         string
	UserID         string
	Action         string
	Status         string
	StartTimestamp int64
	EndTimestamp   int64
	UserIDs        []int
}

func InitTask(platform constant.TaskPlatform, relayInfo *commonRelay.RelayInfo) *Task {
	properties := Properties{}
	privateData := TaskPrivateData{}
	if relayInfo != nil && relayInfo.ChannelMeta != nil {
		// A New API channel may rotate between several gateway tokens, so the
		// task keeps the key that submitted it and polls with the same identity.
		if relayInfo.ChannelMeta.ChannelType == constant.ChannelTypeGemini ||
			relayInfo.ChannelMeta.ChannelType == constant.ChannelTypeVertexAi ||
			relayInfo.ChannelMeta.ChannelType == constant.ChannelTypeNewAPI {
			privateData.Key = relayInfo.ChannelMeta.ApiKey
		}
		if relayInfo.UpstreamModelName != "" {
			properties.UpstreamModelName = relayInfo.UpstreamModelName
		}
		if relayInfo.OriginModelName != "" {
			properties.OriginModelName = relayInfo.OriginModelName
		}
	}

	// 使用预生成的公开 ID（如果有），否则新生成
	taskID := ""
	if relayInfo.TaskRelayInfo != nil && relayInfo.TaskRelayInfo.PublicTaskID != "" {
		taskID = relayInfo.TaskRelayInfo.PublicTaskID
	} else {
		taskID = GenerateTaskID()
	}

	t := &Task{
		TaskID:      taskID,
		UserId:      relayInfo.UserId,
		Group:       relayInfo.UsingGroup,
		SubmitTime:  time.Now().Unix(),
		Status:      TaskStatusNotStart,
		Progress:    "0%",
		ChannelId:   relayInfo.ChannelId,
		Platform:    platform,
		Properties:  properties,
		PrivateData: privateData,
	}
	return t
}

func TaskGetAllUserTask(userId int, startIdx int, num int, queryParams SyncTaskQueryParams) []*Task {
	var tasks []*Task
	var err error

	// 初始化查询构建器
	query := DB.Where("user_id = ?", userId)

	if queryParams.TaskID != "" {
		query = query.Where("task_id = ?", queryParams.TaskID)
	}
	if queryParams.Action != "" {
		query = query.Where("action = ?", queryParams.Action)
	}
	if queryParams.Status != "" {
		query = query.Where("status = ?", queryParams.Status)
	}
	if queryParams.Platform != "" {
		query = query.Where("platform = ?", queryParams.Platform)
	}
	if queryParams.StartTimestamp != 0 {
		// 假设您已将前端传来的时间戳转换为数据库所需的时间格式，并处理了时间戳的验证和解析
		query = query.Where("submit_time >= ?", queryParams.StartTimestamp)
	}
	if queryParams.EndTimestamp != 0 {
		query = query.Where("submit_time <= ?", queryParams.EndTimestamp)
	}

	// 获取数据
	// Task lists never render the persisted upstream snapshot; the dashboard
	// loads media through the artifacts endpoint instead.
	err = query.Omit("channel_id", "data").Order("id desc").Limit(num).Offset(startIdx).Find(&tasks).Error
	if err != nil {
		return nil
	}

	return tasks
}

func TaskGetAllTasks(startIdx int, num int, queryParams SyncTaskQueryParams) []*Task {
	var tasks []*Task
	var err error

	// 初始化查询构建器
	query := DB

	// 添加过滤条件
	if queryParams.ChannelID != "" {
		query = query.Where("channel_id = ?", queryParams.ChannelID)
	}
	if queryParams.Platform != "" {
		query = query.Where("platform = ?", queryParams.Platform)
	}
	if queryParams.UserID != "" {
		query = query.Where("user_id = ?", queryParams.UserID)
	}
	if len(queryParams.UserIDs) != 0 {
		query = query.Where("user_id in (?)", queryParams.UserIDs)
	}
	if queryParams.TaskID != "" {
		query = query.Where("task_id = ?", queryParams.TaskID)
	}
	if queryParams.Action != "" {
		query = query.Where("action = ?", queryParams.Action)
	}
	if queryParams.Status != "" {
		query = query.Where("status = ?", queryParams.Status)
	}
	if queryParams.StartTimestamp != 0 {
		query = query.Where("submit_time >= ?", queryParams.StartTimestamp)
	}
	if queryParams.EndTimestamp != 0 {
		query = query.Where("submit_time <= ?", queryParams.EndTimestamp)
	}

	// 获取数据
	err = query.Omit("data").Order("id desc").Limit(num).Offset(startIdx).Find(&tasks).Error
	if err != nil {
		return nil
	}

	return tasks
}

func GetTimedOutUnfinishedTasks(cutoffUnix int64, limit int) []*Task {
	var tasks []*Task
	err := DB.Where("progress != ?", "100%").
		Where("status NOT IN ?", []string{TaskStatusFailure, TaskStatusSuccess, TaskStatusRejected}).
		Where("submit_time < ?", cutoffUnix).
		Order("submit_time").
		Limit(limit).
		Find(&tasks).Error
	if err != nil {
		return nil
	}
	return tasks
}

func GetStaleUnfinishedImageTasks(cutoffUnix int64, limit int) []*Task {
	var tasks []*Task
	err := DB.Where("image_task = ?", true).
		Where("status IN ?", []string{string(TaskStatusNotStart), string(TaskStatusSubmitted), string(TaskStatusQueued), string(TaskStatusInProgress)}).
		Where("(updated_at = 0 AND created_at < ?) OR (updated_at > 0 AND updated_at < ?)", cutoffUnix, cutoffUnix).
		Order("updated_at").Limit(limit).Find(&tasks).Error
	if err != nil {
		return nil
	}
	return tasks
}

// GetPendingAsyncImageTasks returns queued submissions and stale in-progress
// rows so a process restart cannot leave an accepted request stuck forever.
func GetPendingAsyncImageTasks(recoveryCutoff int64, limit int) ([]*Task, error) {
	if limit <= 0 {
		limit = 100
	}
	var tasks []*Task
	err := DB.Where("async_image = ? AND ((status = ?) OR (status = ? AND updated_at > 0 AND updated_at < ?))", true, TaskStatusQueued, TaskStatusInProgress, recoveryCutoff).
		Order("id").Limit(limit).Find(&tasks).Error
	return tasks, err
}

func GetAllUnFinishSyncTasks(limit int) []*Task {
	var tasks []*Task
	var err error
	// get all tasks progress is not 100%
	err = DB.Where("progress != ?", "100%").Where("status NOT IN ?", []TaskStatus{TaskStatusFailure, TaskStatusSuccess, TaskStatusRejected}).Limit(limit).Order("id").Find(&tasks).Error
	if err != nil {
		return nil
	}
	return tasks
}

// HasUnfinishedSyncTasks reports whether at least one async (Suno/video) task is
// still in progress. It is a cheap existence check (LIMIT 1) used to decide
// whether the async_task_poll system task needs to run; when no task is pending
// the scheduler skips creating a row entirely.
func HasUnfinishedSyncTasks() bool {
	var id int64
	err := DB.Model(&Task{}).
		Where("progress != ?", "100%").
		Where("status NOT IN ?", []TaskStatus{TaskStatusFailure, TaskStatusSuccess, TaskStatusRejected}).
		Limit(1).
		Pluck("id", &id).Error
	return err == nil && id != 0
}

func GetByOnlyTaskId(taskId string) (*Task, bool, error) {
	if taskId == "" {
		return nil, false, nil
	}
	var task *Task
	var err error
	err = DB.Where("task_id = ?", taskId).First(&task).Error
	exist, err := RecordExist(err)
	if err != nil {
		return nil, false, err
	}
	return task, exist, err
}

// GetUniqueByOnlyTaskId resolves a public task identifier only when exactly one
// row owns it. Historical task identifiers were not globally unique, so
// capability-based reads must fail closed instead of selecting an arbitrary
// tenant's row.
func GetUniqueByOnlyTaskId(taskId string) (*Task, bool, error) {
	if taskId == "" {
		return nil, false, nil
	}
	var tasks []*Task
	if err := DB.Where("task_id = ?", taskId).Order("id").Limit(2).Find(&tasks).Error; err != nil {
		return nil, false, err
	}
	if len(tasks) != 1 {
		return nil, false, nil
	}
	return tasks[0], true, nil
}

func GetByTaskId(userId int, taskId string) (*Task, bool, error) {
	if taskId == "" {
		return nil, false, nil
	}
	var task *Task
	var err error
	err = DB.Where("user_id = ? and task_id = ?", userId, taskId).
		First(&task).Error
	exist, err := RecordExist(err)
	if err != nil {
		return nil, false, err
	}
	return task, exist, err
}

func GetByTaskIdsForPlatforms(userID int, platforms []constant.TaskPlatform, taskIDs []string) ([]*Task, error) {
	if len(platforms) == 0 || len(taskIDs) == 0 {
		return nil, nil
	}
	var tasks []*Task
	err := DB.
		Where("user_id = ? AND platform IN ? AND task_id IN ?", userID, platforms, taskIDs).
		Find(&tasks).Error
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

// GetTaskForProtocolObservation reloads one public task through the ownership
// boundary used by long-lived plugin protocol observers. A missing task,
// foreign user, and wrong plugin platform are deliberately indistinguishable.
func GetTaskForProtocolObservation(ctx context.Context, userID int, platform constant.TaskPlatform, taskID string) (*Task, bool, error) {
	if taskID == "" {
		return nil, false, nil
	}
	var task Task
	err := DB.WithContext(ctx).
		Where("user_id = ? AND platform = ? AND task_id = ?", userID, platform, taskID).
		First(&task).Error
	exists, err := RecordExist(err)
	if err != nil || !exists {
		return nil, exists, err
	}
	return &task, true, nil
}

func (Task *Task) Insert() error {
	return Task.InsertWithContext(context.Background())
}

// InsertWithContext creates the row. omitColumns are left out of the INSERT
// (for example "data" when the submit route discards the upstream snapshot)
// while the in-memory task keeps its values for presentation.
func (Task *Task) InsertWithContext(ctx context.Context, omitColumns ...string) error {
	tx := DB.WithContext(ctx)
	if len(omitColumns) > 0 {
		tx = tx.Omit(omitColumns...)
	}
	return tx.Create(Task).Error
}

type taskSnapshot struct {
	Status       TaskStatus
	Progress     string
	StartTime    int64
	FinishTime   int64
	FailReason   string
	ResultURL    string
	Data         json.RawMessage
	PluginState  json.RawMessage
	PollFailures int
}

func (s taskSnapshot) Equal(other taskSnapshot) bool {
	return s.Status == other.Status &&
		s.Progress == other.Progress &&
		s.StartTime == other.StartTime &&
		s.FinishTime == other.FinishTime &&
		s.FailReason == other.FailReason &&
		s.ResultURL == other.ResultURL &&
		bytes.Equal(s.Data, other.Data) &&
		bytes.Equal(s.PluginState, other.PluginState) &&
		s.PollFailures == other.PollFailures
}

func (t *Task) Snapshot() taskSnapshot {
	return taskSnapshot{
		Status:       t.Status,
		Progress:     t.Progress,
		StartTime:    t.StartTime,
		FinishTime:   t.FinishTime,
		FailReason:   t.FailReason,
		ResultURL:    t.PrivateData.ResultURL,
		Data:         t.Data,
		PluginState:  t.PrivateData.PluginState,
		PollFailures: t.PrivateData.PollFailures,
	}
}

func (Task *Task) Update() error {
	var err error
	err = DB.Save(Task).Error
	return err
}

// UpdatePrivateData writes only the private metadata column, so media storage
// bookkeeping cannot overwrite a task status changed by another poll worker.
func (t *Task) UpdatePrivateData() error {
	if t == nil || t.ID == 0 {
		return errors.New("task row is required")
	}
	return DB.Model(&Task{}).Where("id = ?", t.ID).Update("private_data", t.PrivateData).Error
}

// UpdateDataAndPrivateData persists a rewritten image response together with
// its delivery metadata without touching the task lifecycle columns. This is
// used when a previously generated native image is redelivered after a
// transient storage/download failure.
func (t *Task) UpdateDataAndPrivateData() error {
	if t == nil || t.ID == 0 {
		return errors.New("task row is required")
	}
	return DB.Model(&Task{}).Where("id = ?", t.ID).Updates(map[string]any{
		"data":         t.Data,
		"private_data": t.PrivateData,
	}).Error
}

// MarkImageTaskExpired changes only the task lifecycle state. Financial
// settlement remains successful and the task record stays queryable.
func MarkImageTaskExpired(taskID string) (bool, error) {
	result := DB.Model(&Task{}).
		Where("task_id = ? AND status = ?", taskID, TaskStatusSuccess).
		Update("status", TaskStatusExpired)
	return result.RowsAffected > 0, result.Error
}

func (t *Task) UpdateQuota() error {
	return DB.Model(t).Update("quota", t.Quota).Error
}

// UpdateWithStatus performs a conditional UPDATE guarded by fromStatus (CAS).
// Returns (true, nil) if this caller won the update, (false, nil) if
// another process already moved the task out of fromStatus. MySQL commonly
// reports changed rows rather than matched rows, so a same-value no-op update
// can also return false even when the status predicate still matched.
//
// Uses Model().Select("*").Updates() instead of Save() because GORM's Save
// falls back to INSERT ON CONFLICT when the WHERE-guarded UPDATE matches
// zero rows, which silently bypasses the CAS guard.
func (t *Task) UpdateWithStatus(fromStatus TaskStatus) (bool, error) {
	result := DB.Model(t).Where("status = ?", fromStatus).Select("*").Updates(t)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// TaskBulkUpdateByID performs an unconditional bulk UPDATE by primary key IDs.
// WARNING: This function has NO CAS (Compare-And-Swap) guard — it will overwrite
// any concurrent status changes. DO NOT use in billing/quota lifecycle flows
// (e.g., timeout, success, failure transitions that trigger refunds or settlements).
// For status transitions that involve billing, use Task.UpdateWithStatus() instead.
func TaskBulkUpdateByID(ids []int64, params map[string]any) error {
	if len(ids) == 0 {
		return nil
	}
	return DB.Model(&Task{}).
		Where("id in (?)", ids).
		Updates(params).Error
}

type TaskQuotaUsage struct {
	Mode  string  `json:"mode"`
	Count float64 `json:"count"`
}

// TaskCountAllTasks returns total tasks that match the given query params (admin usage)
func TaskCountAllTasks(queryParams SyncTaskQueryParams) int64 {
	var total int64
	query := DB.Model(&Task{})
	if queryParams.ChannelID != "" {
		query = query.Where("channel_id = ?", queryParams.ChannelID)
	}
	if queryParams.Platform != "" {
		query = query.Where("platform = ?", queryParams.Platform)
	}
	if queryParams.UserID != "" {
		query = query.Where("user_id = ?", queryParams.UserID)
	}
	if len(queryParams.UserIDs) != 0 {
		query = query.Where("user_id in (?)", queryParams.UserIDs)
	}
	if queryParams.TaskID != "" {
		query = query.Where("task_id = ?", queryParams.TaskID)
	}
	if queryParams.Action != "" {
		query = query.Where("action = ?", queryParams.Action)
	}
	if queryParams.Status != "" {
		query = query.Where("status = ?", queryParams.Status)
	}
	if queryParams.StartTimestamp != 0 {
		query = query.Where("submit_time >= ?", queryParams.StartTimestamp)
	}
	if queryParams.EndTimestamp != 0 {
		query = query.Where("submit_time <= ?", queryParams.EndTimestamp)
	}
	_ = query.Count(&total).Error
	return total
}

// TaskCountAllUserTask returns total tasks for given user
func TaskCountAllUserTask(userId int, queryParams SyncTaskQueryParams) int64 {
	var total int64
	query := DB.Model(&Task{}).Where("user_id = ?", userId)
	if queryParams.TaskID != "" {
		query = query.Where("task_id = ?", queryParams.TaskID)
	}
	if queryParams.Action != "" {
		query = query.Where("action = ?", queryParams.Action)
	}
	if queryParams.Status != "" {
		query = query.Where("status = ?", queryParams.Status)
	}
	if queryParams.Platform != "" {
		query = query.Where("platform = ?", queryParams.Platform)
	}
	if queryParams.StartTimestamp != 0 {
		query = query.Where("submit_time >= ?", queryParams.StartTimestamp)
	}
	if queryParams.EndTimestamp != 0 {
		query = query.Where("submit_time <= ?", queryParams.EndTimestamp)
	}
	_ = query.Count(&total).Error
	return total
}
func (t *Task) ToOpenAIVideo() *dto.OpenAIVideo {
	openAIVideo := dto.NewOpenAIVideo()
	openAIVideo.ID = t.TaskID
	openAIVideo.Status = t.Status.ToVideoStatus()
	openAIVideo.Model = t.Properties.OriginModelName
	openAIVideo.SetProgressStr(t.Progress)
	openAIVideo.CreatedAt = t.CreatedAt
	if t.Status == TaskStatusSuccess {
		if t.FinishTime != 0 {
			openAIVideo.CompletedAt = t.FinishTime
		} else {
			openAIVideo.CompletedAt = t.UpdatedAt
		}
	}
	return openAIVideo
}
