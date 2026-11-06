package service

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/system_setting"
	"github.com/shirou/gopsutil/disk"
)

const (
	taskOpsAlertRecipient       = "978206830@qq.com"
	taskOpsAlertCooldown        = 15 * time.Minute
	taskOpsMonitorInterval      = time.Minute
	taskOpsDailyReportHour      = 6
	taskOpsSuccessRateThreshold = 0.95
	taskOpsP0DiskPercent        = 95
	taskOpsP2DiskPercent        = 80
)

var (
	taskOpsMonitorStarted atomic.Bool
	taskQuerySlowCount    atomic.Int64
	taskQuerySlowMaxMs    atomic.Int64
)

type taskOpsAlertState struct {
	last                 map[string]time.Time
	serviceHealthFailure int
	lastHeartbeat        time.Time
}

// RecordTaskQueryDuration is called by the task query handlers. It feeds only
// the private operations monitor and never changes the API response.
func RecordTaskQueryDuration(duration time.Duration) {
	ms := duration.Milliseconds()
	if ms <= 3000 {
		return
	}
	taskQuerySlowCount.Add(1)
	for {
		old := taskQuerySlowMaxMs.Load()
		if old >= ms || taskQuerySlowMaxMs.CompareAndSwap(old, ms) {
			return
		}
	}
}

// StartTaskOperationsMonitor starts the operator-only alert/report loop. It is
// master-only so multiple application instances cannot duplicate email alerts.
func StartTaskOperationsMonitor(port string) {
	if !common.IsMasterNode || !taskOpsMonitorStarted.CompareAndSwap(false, true) {
		return
	}
	go runTaskOperationsMonitor(port)
}

func runTaskOperationsMonitor(port string) {
	state := taskOpsAlertState{last: make(map[string]time.Time)}
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		location = time.Local
	}
	ticker := time.NewTicker(taskOpsMonitorInterval)
	defer ticker.Stop()
	lastReportDay := ""
	for {
		now := time.Now().In(location)
		checkTaskOperations(context.Background(), port, now, &state)
		day := now.Format("2006-01-02")
		if now.Hour() == taskOpsDailyReportHour && lastReportDay != day {
			sendTaskDailyReport(now)
			lastReportDay = day
		}
		<-ticker.C
	}
}

func checkTaskOperations(ctx context.Context, port string, now time.Time, state *taskOpsAlertState) {
	diskUsage, diskPath := taskArtifactDiskUsage()
	cleanupOK, cleanupDetail := imageCleanupHealth(now.Unix())
	diskDetail := fmt.Sprintf("图片存储磁盘 %s 使用 %.1f%%", diskPath, diskUsage)
	if diskUsage >= taskOpsP0DiskPercent {
		if cleanupOK {
			sendTaskOpsAlert(state, "P0: 磁盘使用率达到 95%（清理正常，需扩容）", diskDetail)
		} else {
			sendTaskOpsAlert(state, "P0: 磁盘使用率达到 95%（清理异常，请先修复清理）", diskDetail+"；"+cleanupDetail)
		}
	} else if diskUsage >= taskOpsP2DiskPercent {
		if cleanupOK {
			sendTaskOpsAlert(state, "P2: 磁盘使用率达到 80%（清理正常）", diskDetail)
		} else {
			sendTaskOpsAlert(state, "P2: 磁盘使用率达到 80%（清理异常）", diskDetail+"；"+cleanupDetail)
		}
	}

	stats, err := model.GetImageTaskHealthStats(now.Add(-5*time.Minute).Unix(), now.Unix())
	if err == nil && stats.Ended >= 20 {
		successRate := float64(stats.Succeeded) / float64(stats.Ended)
		if successRate < taskOpsSuccessRateThreshold {
			sendTaskOpsAlert(state, "P1: 最近 5 分钟图片成功率低于 95%", fmt.Sprintf("成功率 %.2f%%；上游失败 %d；我方失败 %d；未归因 %d", successRate*100, stats.UpstreamFailures, stats.LocalFailures, stats.Unclassified))
		}
		if stats.Unclassified*5 > stats.Failed {
			sendTaskOpsAlert(state, "P1: 图片失败未归因比例超过 20%", fmt.Sprintf("失败 %d；未归因 %d", stats.Failed, stats.Unclassified))
		}
	}

	if !cleanupOK {
		sendTaskOpsAlert(state, "P1: 图片清理任务超过 2 小时未正常完成", cleanupDetail)
	}
	if slow := taskQuerySlowMaxMs.Swap(0); slow > 3000 {
		count := taskQuerySlowCount.Swap(0)
		sendTaskOpsAlert(state, "P2: 单次任务查询超过 3 秒", fmt.Sprintf("最近窗口 %d 次，最慢 %d ms", count, slow))
	}
	checkBackupHealth(now.Unix(), state)
	checkServiceHealth(port, state)
	if state.lastHeartbeat.IsZero() || now.Sub(state.lastHeartbeat) >= time.Hour {
		checkExternalHeartbeat(ctx, state)
		state.lastHeartbeat = now
	}
}

func taskArtifactDiskUsage() (float64, string) {
	config := system_setting.LoadTaskArtifactStoreConfig()
	if config.Mode == system_setting.TaskArtifactStoreModeLocal && config.LocalRoot != "" {
		path, err := filepath.Abs(config.LocalRoot)
		if err == nil {
			usage, usageErr := disk.Usage(path)
			if usageErr != nil {
				usage, usageErr = disk.Usage(filepath.Dir(path))
			}
			if usageErr == nil {
				return usage.UsedPercent, path
			}
		}
	}
	info := common.GetDiskSpaceInfo()
	return info.UsedPercent, common.GetDiskCachePath()
}

func imageCleanupHealth(now int64) (bool, string) {
	task, err := model.GetLatestSystemTask(model.SystemTaskTypeImageCleanup)
	if err != nil {
		return false, "清理任务状态读取失败"
	}
	if task == nil {
		return false, "尚未有清理任务记录"
	}
	if task.Status != model.SystemTaskStatusSucceeded || now-task.UpdatedAt > int64((2*time.Hour).Seconds()) {
		return false, fmt.Sprintf("清理任务状态 %s，最近更新时间 %s", task.Status, time.Unix(task.UpdatedAt, 0).Format(time.RFC3339))
	}
	return true, "清理任务正常"
}

func checkServiceHealth(port string, state *taskOpsAlertState) {
	if strings.TrimSpace(port) == "" {
		return
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/api/status")
	if err == nil {
		_ = resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 500 {
			state.serviceHealthFailure = 0
			return
		}
	}
	state.serviceHealthFailure++
	if state.serviceHealthFailure >= 2 {
		sendTaskOpsAlert(state, "P0: 服务健康检查连续失败", "本机 /api/status 已连续两分钟不可用；服务可能已停止或无法接受请求")
	}
}

func checkExternalHeartbeat(ctx context.Context, state *taskOpsAlertState) {
	endpoint := strings.TrimSpace(os.Getenv("TASK_HEARTBEAT_URL"))
	if endpoint == "" {
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		sendTaskOpsAlert(state, "P0: 外部心跳发送失败", "心跳地址配置无效")
		return
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		sendTaskOpsAlert(state, "P0: 外部心跳发送失败", "无法访问 Healthchecks/UptimeRobot 心跳地址")
		return
	}
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		sendTaskOpsAlert(state, "P0: 外部心跳发送失败", fmt.Sprintf("心跳服务返回 HTTP %d", resp.StatusCode))
	}
}

func checkBackupHealth(now int64, state *taskOpsAlertState) {
	path := strings.TrimSpace(os.Getenv("TASK_BACKUP_STATUS_FILE"))
	if path == "" {
		return
	}
	info, err := os.Stat(filepath.Clean(path))
	if err != nil {
		sendTaskOpsAlert(state, "P1: 数据库备份状态不可用", "备份状态文件不存在或无法读取")
		return
	}
	maxAge := int64(common.GetEnvOrDefault("TASK_BACKUP_MAX_AGE_SECONDS", 36*60*60))
	if maxAge > 0 && now-info.ModTime().Unix() > maxAge {
		sendTaskOpsAlert(state, "P1: 数据库备份失败或过期", fmt.Sprintf("备份状态文件已超过 %d 秒未更新", maxAge))
	}
}

func sendTaskOpsAlert(state *taskOpsAlertState, subject, detail string) {
	if state == nil {
		return
	}
	now := time.Now()
	if last := state.last[subject]; !last.IsZero() && now.Sub(last) < taskOpsAlertCooldown {
		return
	}
	state.last[subject] = now
	content := fmt.Sprintf("<h3>%s</h3><p>%s</p><p>节点：%s</p><p>时间：%s</p>", subject, detail, common.NodeName, now.Format(time.RFC3339))
	if err := common.SendEmail(subject, taskOpsAlertRecipient, content); err != nil {
		logger.LogError(context.Background(), common.LogText("task operations alert email failed: %v", err))
	}
}

func sendTaskDailyReport(now time.Time) {
	stats, err := model.GetImageTaskHealthStats(now.Add(-24*time.Hour).Unix(), now.Unix())
	if err != nil {
		sendTaskOpsAlert(&taskOpsAlertState{last: make(map[string]time.Time)}, "P1: 每日报告生成失败", err.Error())
		return
	}
	successRate := 0.0
	if stats.Ended > 0 {
		successRate = float64(stats.Succeeded) / float64(stats.Ended) * 100
	}
	diskUsage, diskPath := taskArtifactDiskUsage()
	_, cleanupDetail := imageCleanupHealth(now.Unix())
	content := fmt.Sprintf("<h3>图片中转站每日运行报告</h3><p>统计时间：%s</p><p>已结束任务：%d；成功交付/已成功任务：%d；失败：%d；内容拒绝：%d；成功率：%.2f%%</p><p>上游失败：%d；我方失败：%d；未归因：%d</p><p>图片存储磁盘 %s 使用：%.1f%%；%s</p>", now.Format(time.RFC3339), stats.Ended, stats.Succeeded, stats.Failed, stats.Rejected, successRate, stats.UpstreamFailures, stats.LocalFailures, stats.Unclassified, diskPath, diskUsage, cleanupDetail)
	if err := common.SendEmail("图片中转站每日运行报告", taskOpsAlertRecipient, content); err != nil {
		logger.LogError(context.Background(), common.LogText("daily task operations report email failed: %v", err))
	}
}
