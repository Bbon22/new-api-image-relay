package model

import (
	"errors"
	"time"

	"gorm.io/gorm"
)

const (
	TaskIdempotencyProcessing = "processing"
	TaskIdempotencyAccepted   = "accepted"
	TaskIdempotencyFailed    = "failed"
)

// TaskIdempotency reserves an image submission before it can reach an
// upstream. The raw client key is never stored; the composite unique index
// makes concurrent retries across app instances converge on one task.
type TaskIdempotency struct {
	ID          int64  `json:"id" gorm:"primaryKey;autoIncrement"`
	TokenID     int    `json:"token_id" gorm:"uniqueIndex:idx_task_idempotency_token_key,priority:1;index"`
	KeyHash     string `json:"key_hash" gorm:"type:char(64);size:64;uniqueIndex:idx_task_idempotency_token_key,priority:2"`
	RequestHash string `json:"request_hash" gorm:"type:char(64);size:64"`
	TaskID      string `json:"task_id" gorm:"type:varchar(191);index"`
	State       string `json:"state" gorm:"type:varchar(20);index"`
	ErrorCode   string `json:"error_code" gorm:"type:varchar(64)"`
	ExpiresAt   int64  `json:"expires_at" gorm:"index"`
	CreatedAt   int64  `json:"created_at"`
	// Synchronous image relays store the already-sanitized response here so a
	// client retry can be served without invoking or charging the provider a
	// second time. The JSON field is intentionally omitted from API responses.
	ResponseBody        []byte `json:"-"`
	ResponseStatus      int    `json:"-"`
	ResponseContentType string `json:"-" gorm:"type:varchar(255)"`
}

// ReserveTaskIdempotency atomically claims a token-scoped key for ten minutes.
// An active existing record is returned with created=false for replay handling.
func ReserveTaskIdempotency(record *TaskIdempotency, now int64) (*TaskIdempotency, bool, error) {
	if record == nil || record.TokenID <= 0 || record.KeyHash == "" || record.RequestHash == "" || record.ExpiresAt <= now {
		return nil, false, errors.New("invalid task idempotency reservation")
	}
	if err := DB.Where("token_id = ? AND key_hash = ? AND expires_at <= ?", record.TokenID, record.KeyHash, now).
		Delete(&TaskIdempotency{}).Error; err != nil {
		return nil, false, err
	}
	record.CreatedAt = now
	record.State = TaskIdempotencyProcessing
	if err := DB.Create(record).Error; err == nil {
		return record, true, nil
	}
	var existing TaskIdempotency
	err := DB.Where("token_id = ? AND key_hash = ? AND expires_at > ?", record.TokenID, record.KeyHash, now).
		First(&existing).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, err
	}
	if err != nil {
		return nil, false, err
	}
	return &existing, false, nil
}

func UpdateTaskIdempotency(tokenID int, keyHash, requestHash, state, errorCode string) error {
	return DB.Model(&TaskIdempotency{}).
		Where("token_id = ? AND key_hash = ? AND request_hash = ?", tokenID, keyHash, requestHash).
		Updates(map[string]any{"state": state, "error_code": errorCode}).Error
}

func RefreshTaskIdempotencyReservation(id int64, expiresAt int64) error {
	if id <= 0 || expiresAt <= time.Now().Unix() {
		return errors.New("invalid task idempotency lease")
	}
	result := DB.Model(&TaskIdempotency{}).
		Where("id = ? AND state = ?", id, TaskIdempotencyProcessing).
		Update("expires_at", expiresAt)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return errors.New("image idempotency reservation is no longer processing")
	}
	return nil
}

// CompleteTaskIdempotencyResponse atomically publishes a replayable response
// after the synchronous image relay has fully rendered it to the client.
func CompleteTaskIdempotencyResponse(id int64, status int, contentType string, body []byte) error {
	if id <= 0 || status < 100 || len(body) == 0 {
		return errors.New("invalid task idempotency response")
	}
	result := DB.Model(&TaskIdempotency{}).
		Where("id = ? AND state = ?", id, TaskIdempotencyProcessing).
		Updates(map[string]any{
			"state":                TaskIdempotencyAccepted,
			"error_code":           "",
			"response_body":        body,
			"response_status":      status,
			"response_content_type": contentType,
			"expires_at":           time.Now().Add(10 * time.Minute).Unix(),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return errors.New("image idempotency reservation is no longer processing")
	}
	return nil
}

// FailTaskIdempotency prevents a retry from re-submitting a request that has
// already reached a terminal relay outcome. The public error is reconstructed
// from the safe error code by the controller.
func FailTaskIdempotency(id int64, errorCode string) error {
	if id <= 0 {
		return errors.New("invalid task idempotency id")
	}
	result := DB.Model(&TaskIdempotency{}).
		Where("id = ? AND state = ?", id, TaskIdempotencyProcessing).
		Updates(map[string]any{
			"state":      TaskIdempotencyFailed,
			"error_code": errorCode,
			"expires_at": time.Now().Add(10 * time.Minute).Unix(),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return errors.New("image idempotency reservation is no longer processing")
	}
	return nil
}

func CleanupTaskIdempotency(now time.Time) (int64, error) {
	result := DB.Where("expires_at <= ?", now.Unix()).Delete(&TaskIdempotency{})
	return result.RowsAffected, result.Error
}
