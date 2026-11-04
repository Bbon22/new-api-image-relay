package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/service"
	"github.com/gin-gonic/gin"
)

const syncImageIdempotencyTTL = 10 * time.Minute

type imageIdempotencyWriter struct {
	gin.ResponseWriter
	body       bytes.Buffer
	status     int
	maxBody    int
	bodyTooBig bool
	panicked   bool
}

func (w *imageIdempotencyWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *imageIdempotencyWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if !w.bodyTooBig {
		if w.body.Len()+len(body) > w.maxBody {
			w.bodyTooBig = true
			w.body.Reset()
		} else {
			_, _ = w.body.Write(body)
		}
	}
	return w.ResponseWriter.Write(body)
}

func (w *imageIdempotencyWriter) WriteString(value string) (int, error) {
	return w.Write([]byte(value))
}

type syncImageIdempotency struct {
	record    *model.TaskIdempotency
	writer    *imageIdempotencyWriter
	stopLease context.CancelFunc
}

// beginSyncImageIdempotency reserves a token-scoped key before billing or an
// upstream call. It applies only to the ordinary synchronous image relay; the
// task-plugin bridge has its own durable task idempotency lifecycle.
func beginSyncImageIdempotency(c *gin.Context) (*syncImageIdempotency, bool) {
	if c.GetBool("hosted_image_protocol") {
		return nil, false
	}
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
		respondSyncImageIdempotencyError(c, http.StatusBadRequest, "Invalid image request", "invalid_request")
		return nil, true
	}
	body, err := storage.Bytes()
	if err != nil {
		respondSyncImageIdempotencyError(c, http.StatusBadRequest, "Invalid image request", "invalid_request")
		return nil, true
	}
	if imageRequestUsesStreaming(body, c.GetHeader("Content-Type")) {
		respondSyncImageIdempotencyError(c, http.StatusBadRequest, "Idempotency-Key is not supported for streaming image requests", "idempotency_stream_unsupported")
		return nil, true
	}
	keyDigest := sha256.Sum256([]byte(rawKey))
	mediaType, _, _ := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if strings.EqualFold(mediaType, "multipart/form-data") {
		body, err = canonicalImageMultipartPayload(body, c.GetHeader("Content-Type"))
		if err != nil {
			respondSyncImageIdempotencyError(c, http.StatusBadRequest, "Invalid image request", "invalid_request")
			return nil, true
		}
	} else if (strings.EqualFold(mediaType, "application/json") || strings.HasSuffix(strings.ToLower(mediaType), "+json")) && len(body) <= 256<<10 {
		var payload any
		if err = common.Unmarshal(body, &payload); err != nil {
			respondSyncImageIdempotencyError(c, http.StatusBadRequest, "Invalid image request", "invalid_request")
			return nil, true
		}
		body, err = common.Marshal(payload)
		if err != nil {
			respondSyncImageIdempotencyError(c, http.StatusBadRequest, "Invalid image request", "invalid_request")
			return nil, true
		}
	}
	hashInput := append([]byte(c.Request.Method+"\x00"+c.Request.URL.Path+"\x00"+c.Request.URL.RawQuery+"\x00"+strings.ToLower(mediaType)+"\x00"+c.GetHeader("Accept")+"\x00"), body...)
	bodyDigest := sha256.Sum256(hashInput)
	record := &model.TaskIdempotency{
		TokenID:     c.GetInt("token_id"),
		KeyHash:     hex.EncodeToString(keyDigest[:]),
		RequestHash: hex.EncodeToString(bodyDigest[:]),
		ExpiresAt:   time.Now().Add(syncImageIdempotencyTTL).Unix(),
	}
	existing, created, err := model.ReserveTaskIdempotency(record, time.Now().Unix())
	if err != nil {
		respondSyncImageIdempotencyError(c, http.StatusInternalServerError, "Request could not be accepted", "idempotency_error")
		return nil, true
	}
	if !created {
		if existing.RequestHash != record.RequestHash {
			respondSyncImageIdempotencyError(c, http.StatusConflict, "Idempotency-Key conflicts with an earlier request", "idempotency_conflict")
			return nil, true
		}
		if existing.State == model.TaskIdempotencyAccepted && existing.ResponseStatus >= 100 && len(existing.ResponseBody) > 0 {
			writeSyncImageIdempotencyReplay(c, existing)
			return nil, true
		}
		if existing.State == model.TaskIdempotencyFailed {
			writeSyncImageIdempotencyFailure(c, existing.ErrorCode)
			return nil, true
		}
		c.Header("Retry-After", "3")
		respondSyncImageIdempotencyError(c, http.StatusConflict, "请求仍在处理中，请稍后使用相同 Idempotency-Key 重试", "idempotency_in_progress")
		return nil, true
	}

	writer := &imageIdempotencyWriter{ResponseWriter: c.Writer, maxBody: 16 << 20}
	c.Writer = writer
	leaseCtx, stopLease := context.WithCancel(context.Background())
	go refreshSyncImageIdempotencyLease(leaseCtx, record.ID)
	return &syncImageIdempotency{record: record, writer: writer, stopLease: stopLease}, false
}

func canonicalImageMultipartPayload(body []byte, contentType string) ([]byte, error) {
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil || !strings.EqualFold(mediaType, "multipart/form-data") || params["boundary"] == "" {
		return nil, fmt.Errorf("invalid multipart content type")
	}
	reader := multipart.NewReader(bytes.NewReader(body), params["boundary"])
	parts := make([]string, 0, 8)
	for {
		part, nextErr := reader.NextPart()
		if nextErr == io.EOF {
			break
		}
		if nextErr != nil {
			return nil, nextErr
		}
		digest := sha256.New()
		partSize, readErr := io.Copy(digest, io.LimitReader(part, 24<<20+1))
		_ = part.Close()
		if readErr != nil {
			return nil, readErr
		}
		if partSize > 24<<20 {
			return nil, fmt.Errorf("multipart body exceeds the allowed size")
		}
		parts = append(parts, fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%x", part.FormName(), part.FileName(), part.Header.Get("Content-Type"), partSize, digest.Sum(nil)))
	}
	sort.Strings(parts)
	return []byte(strings.Join(parts, "\n")), nil
}

func finishSyncImageIdempotency(idem *syncImageIdempotency) {
	if idem == nil || idem.record == nil {
		return
	}
	if idem.stopLease != nil {
		idem.stopLease()
	}
	if idem.writer == nil || idem.writer.panicked || idem.writer.bodyTooBig || idem.writer.body.Len() == 0 {
		if err := model.FailTaskIdempotency(idem.record.ID, "response_not_replayable"); err != nil {
			logger.LogError(context.Background(), common.LogText("failed to close image idempotency record: %s", err.Error()))
		}
		return
	}
	contentType := idem.writer.Header().Get("Content-Type")
	if err := model.CompleteTaskIdempotencyResponse(idem.record.ID, idem.writer.status, contentType, idem.writer.body.Bytes()); err != nil {
		logger.LogError(context.Background(), common.LogText("failed to save image idempotency response: %s", err.Error()))
	}
}

func refreshSyncImageIdempotencyLease(ctx context.Context, recordID int64) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := model.RefreshTaskIdempotencyReservation(recordID, time.Now().Add(syncImageIdempotencyTTL).Unix()); err != nil {
				logger.LogError(ctx, common.LogText("failed to refresh image idempotency lease: %s", err.Error()))
			}
		}
	}
}

func imageRequestUsesStreaming(body []byte, contentType string) bool {
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false
	}
	if strings.EqualFold(mediaType, "application/json") || strings.HasSuffix(strings.ToLower(mediaType), "+json") {
		var request struct {
			Stream bool `json:"stream"`
		}
		return json.Unmarshal(body, &request) == nil && request.Stream
	}
	if !strings.EqualFold(mediaType, "multipart/form-data") || params["boundary"] == "" {
		return false
	}
	reader := multipart.NewReader(bytes.NewReader(body), params["boundary"])
	for {
		part, nextErr := reader.NextPart()
		if nextErr != nil {
			return false
		}
		if part.FormName() == "stream" {
			value, _ := io.ReadAll(io.LimitReader(part, 16))
			_ = part.Close()
			return strings.EqualFold(strings.TrimSpace(string(value)), "true")
		}
		_ = part.Close()
	}
}

func writeSyncImageIdempotencyReplay(c *gin.Context, record *model.TaskIdempotency) {
	contentType := record.ResponseContentType
	if contentType == "" {
		contentType = "application/json; charset=utf-8"
	}
	c.Header("Cache-Control", "private, no-store")
	c.Data(record.ResponseStatus, contentType, record.ResponseBody)
}

func writeSyncImageIdempotencyFailure(c *gin.Context, code string) {
	if code == string(types.ErrorCodePromptBlocked) {
		respondSyncImageIdempotencyError(c, http.StatusBadRequest, service.TaskContentRejectedMessage, "prompt_blocked")
		return
	}
	respondSyncImageIdempotencyError(c, http.StatusBadGateway, "当前生成服务繁忙，请稍后重试", "image_generation_failed")
}

func respondSyncImageIdempotencyError(c *gin.Context, status int, message, code string) {
	c.AbortWithStatusJSON(status, gin.H{"error": gin.H{
		"message": message,
		"type":    "new_api_error",
		"code":    code,
	}})
}
