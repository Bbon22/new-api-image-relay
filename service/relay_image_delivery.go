package service

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/setting/system_setting"
	"github.com/gin-gonic/gin"
)

const (
	relayImageRetention = 9 * time.Hour
	// Customer-supplied reference media is only needed while the upstream
	// image-edit request is being submitted. Keep it short-lived so these
	// temporary inputs are removed promptly; generated artifacts retain their
	// separate 9-hour policy above.
	relayImageReferenceRetention       = 8 * time.Minute
	relayImageMaxBytes           int64 = 8 << 20
	relayImageReferenceMaxBytes  int64 = 4 << 20
	relayImageMaxResponse              = 128 << 20
	relayImageArtifactKey              = "image"
	relayImageReferenceKey             = "reference"
	relayImageReferencePrefix          = "ref_"
	relayImageRewriteSlots             = 16
	// When an upstream includes both URL and Base64, let the URL lead briefly,
	// then race the local decode against the URL. The first valid representation
	// wins, so a fixed fallback timeout cannot make delivery slower.
	relayImageBase64RaceDelay = 250 * time.Millisecond
	// Provider image URLs can be served from a cold CDN edge. Keep enough
	// budget for a full 8 MB artifact while still bounding a dead URL.
	relayImageDownloadAttemptTimeout = 20 * time.Second
	relayImageDownloadTotalTimeout   = 65 * time.Second
)

var (
	ErrRelayImageDeliveryFailed = errors.New("image media delivery failed")
	ErrRelayImageRejected       = errors.New("image generation rejected")
	ErrRelayImageGeneration     = errors.New("image generation failed")
	relayImageErrorURLPattern   = regexp.MustCompile(`(?i)(?:https?|wss?)://[^\s"'<>]+`)
	relayImageBearerPattern     = regexp.MustCompile(`(?i)\bbearer\s+[^\s,;]+`)
	relayImageAPIKeyPattern     = regexp.MustCompile(`(?i)\bapi[_-]?key\s*[:=]\s*[^\s,;]+`)
	relayImageSecretKeyPattern  = regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{16,}\b`)

	relayImageStoreOnce sync.Once
	relayImageStoreInst *relayImageStore
	relayImageSlots     = make(chan struct{}, relayImageRewriteSlots)
)

type relayImageStore struct {
	root        string
	baseAddress string
	client      *http.Client
}

func newRelayImageStore(root, baseAddress string, client *http.Client) *relayImageStore {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		absoluteRoot = ""
	}
	if client == nil {
		client = newRelayImageHTTPClient()
	}
	return &relayImageStore{
		root:        filepath.Join(filepath.Clean(absoluteRoot), "relay-images"),
		baseAddress: strings.TrimRight(strings.TrimSpace(baseAddress), "/"),
		client:      client,
	}
}

func getRelayImageStore() *relayImageStore {
	relayImageStoreOnce.Do(func() {
		config := system_setting.LoadTaskArtifactStoreConfig()
		if config.Mode != system_setting.TaskArtifactStoreModeLocal || config.LocalRoot == "" {
			return
		}
		baseAddress := strings.TrimSpace(system_setting.TaskPublicAddress)
		if baseAddress == "" {
			baseAddress = common.GetEnvOrDefaultString("TASK_PUBLIC_ADDRESS", "")
		}
		if baseAddress == "" {
			baseAddress = system_setting.ServerAddress
		}
		relayImageStoreInst = newRelayImageStore(config.LocalRoot, baseAddress, nil)
		go relayImageStoreInst.cleanupLoop()
	})
	return relayImageStoreInst
}

// RewriteRelayImageResponse replaces customer-facing image URLs with signed
// links to locally stored copies. It must run before an adaptor writes a body.
func RewriteRelayImageResponse(ctx context.Context, body []byte, contentType string) ([]byte, error) {
	store := getRelayImageStore()
	if store == nil || store.root == "" {
		return nil, ErrTaskArtifactStoreDisabled
	}
	return store.RewriteResponse(ctx, body, contentType)
}

// StoreRelayImageReference stores customer-provided reference media behind a
// short-lived signed URL for upstream providers that accept URL references.
func StoreRelayImageReference(ctx context.Context, imageBytes []byte) (string, error) {
	store := getRelayImageStore()
	if store == nil || store.root == "" {
		return "", ErrTaskArtifactStoreDisabled
	}
	return store.persistWithPolicy(imageBytes, relayImageReferenceKey, relayImageReferenceRetention, relayImageReferencePrefix)
}

// ValidateRelayImageReferenceBytes validates the size and image header before
// a reference is persisted.
func ValidateRelayImageReferenceBytes(imageBytes []byte) error {
	if len(imageBytes) == 0 || int64(len(imageBytes)) > relayImageReferenceMaxBytes {
		return errors.New("reference image size is invalid")
	}
	return validateRelayImageBytes(imageBytes)
}

// FetchRelayImageReference resolves a customer-supplied public URL, Base64
// payload, or data URL into a validated image without persisting it.
func FetchRelayImageReference(ctx context.Context, input string) ([]byte, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, errors.New("reference image is empty")
	}
	var imageBytes []byte
	var err error
	if strings.HasPrefix(strings.ToLower(input), "http://") || strings.HasPrefix(strings.ToLower(input), "https://") {
		store := getRelayImageStore()
		if store == nil || store.root == "" {
			return nil, ErrTaskArtifactStoreDisabled
		}
		imageBytes, err = store.downloadImageWithRetryLimit(ctx, input, relayImageReferenceMaxBytes)
	} else {
		imageBytes, err = decodeRelayImageLimit(input, relayImageReferenceMaxBytes)
	}
	if err != nil {
		return nil, errors.New("reference image could not be read")
	}
	if err := ValidateRelayImageReferenceBytes(imageBytes); err != nil {
		return nil, errors.New("reference image is invalid")
	}
	return imageBytes, nil
}

// HostRelayImageReference accepts a public URL, a Base64 string, or a data URL,
// then stores the validated bytes on this relay and returns its signed URL.
func HostRelayImageReference(ctx context.Context, input string) (string, error) {
	imageBytes, err := FetchRelayImageReference(ctx, input)
	if err != nil {
		return "", err
	}
	return StoreRelayImageReference(ctx, imageBytes)
}

func AcquireRelayImageRewrite(ctx context.Context) (func(), bool) {
	select {
	case relayImageSlots <- struct{}{}:
		return func() { <-relayImageSlots }, true
	case <-ctx.Done():
		return nil, false
	}
}

func (s *relayImageStore) RewriteResponse(ctx context.Context, body []byte, contentType string) ([]byte, error) {
	if int64(len(body)) > relayImageMaxResponse {
		return nil, fmt.Errorf("image response exceeds %d bytes", relayImageMaxResponse)
	}
	if strings.Contains(strings.ToLower(contentType), "text/event-stream") {
		return s.rewriteEventStream(ctx, body)
	}
	if !isJSONContentType(contentType) {
		return nil, ErrRelayImageDeliveryFailed
	}
	return s.rewriteJSON(ctx, body, false)
}

func isJSONContentType(contentType string) bool {
	contentType = strings.ToLower(strings.TrimSpace(strings.SplitN(contentType, ";", 2)[0]))
	return contentType == "application/json" || strings.HasSuffix(contentType, "+json")
}

func (s *relayImageStore) rewriteJSON(ctx context.Context, body []byte, topLevelImage bool) ([]byte, error) {
	if !json.Valid(body) {
		return nil, errors.New("invalid image response body")
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("invalid image response body: %w", err)
	}
	if failure := relayImageResponseFailure(payload, body); failure != nil {
		return nil, failure
	}

	topLevelPayloadFound := false
	if topLevelImage {
		rewritten, changed, err := s.rewriteImageObject(ctx, mustMarshalRawObject(payload))
		if err != nil {
			return nil, err
		}
		if changed {
			topLevelPayloadFound = true
			var rewrittenPayload map[string]json.RawMessage
			if err := json.Unmarshal(rewritten, &rewrittenPayload); err != nil {
				return nil, err
			}
			payload = rewrittenPayload
		}
	}
	imageContainerFound := false
	imagePayloadFound := false
	for key, rawValue := range payload {
		if !isRelayImageContainerKey(strings.ToLower(key)) {
			continue
		}
		imageContainerFound = true
		rewritten, found, err := s.rewriteImageValue(ctx, rawValue)
		if err != nil {
			return nil, err
		}
		if found {
			imagePayloadFound = true
			payload[key] = rewritten
		}
	}
	if imageContainerFound && !imagePayloadFound {
		return nil, ErrRelayImageDeliveryFailed
	}
	if !imagePayloadFound && !topLevelImage {
		return nil, ErrRelayImageDeliveryFailed
	}
	if topLevelImage && !imagePayloadFound && !topLevelPayloadFound {
		var event struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(body, &event)
		if strings.HasSuffix(strings.ToLower(event.Type), ".completed") {
			return nil, ErrRelayImageDeliveryFailed
		}
	}

	return json.Marshal(payload)
}

func isRelayImageContainerKey(key string) bool {
	switch key {
	case "data", "images", "output", "result", "results", "artifacts", "content":
		return true
	default:
		return false
	}
}

func (s *relayImageStore) rewriteImageValue(ctx context.Context, raw json.RawMessage) (json.RawMessage, bool, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return raw, false, nil
	}
	switch raw[0] {
	case '[':
		var items []json.RawMessage
		if err := json.Unmarshal(raw, &items); err != nil {
			return nil, false, errors.New("invalid image response data")
		}
		found := false
		for i, item := range items {
			rewritten, changed, err := s.rewriteImageValue(ctx, item)
			if err != nil {
				return nil, false, err
			}
			if changed {
				items[i] = rewritten
				found = true
			}
		}
		if !found {
			return raw, false, nil
		}
		encoded, err := json.Marshal(items)
		return encoded, true, err
	case '{':
		if rewritten, changed, err := s.rewriteImageObject(ctx, raw); err != nil || changed {
			return rewritten, changed, err
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return nil, false, err
		}
		found := false
		for key, value := range fields {
			if !isRelayImageContainerKey(strings.ToLower(key)) {
				continue
			}
			rewritten, changed, err := s.rewriteImageValue(ctx, value)
			if err != nil {
				return nil, false, err
			}
			if changed {
				fields[key] = rewritten
				found = true
			}
		}
		if !found {
			return raw, false, nil
		}
		encoded, err := json.Marshal(fields)
		return encoded, true, err
	default:
		return raw, false, nil
	}
}

func mustMarshalRawObject(payload map[string]json.RawMessage) json.RawMessage {
	data, _ := json.Marshal(payload)
	return data
}

func (s *relayImageStore) rewriteImageObject(ctx context.Context, raw json.RawMessage) (json.RawMessage, bool, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return raw, false, nil
	}

	var imageURL string
	_ = json.Unmarshal(fields["url"], &imageURL)
	if imageURL == "" {
		_ = json.Unmarshal(fields["image_url"], &imageURL)
	}
	b64Field := fields["b64_json"]
	hasEncodedImage := len(b64Field) > 0 && string(b64Field) != "null" && string(b64Field) != `""`
	if !hasEncodedImage && imageURL == "" {
		return raw, false, nil
	}

	var imageBytes []byte
	var err error
	// Prefer a regular URL when both representations are present. It avoids
	// copying and decoding a potentially large Base64 response in the common
	// case, while retaining Base64 as a compatibility fallback.
	if imageURL != "" {
		if strings.HasPrefix(strings.ToLower(imageURL), "data:image/") {
			imageBytes, err = decodeRelayImage(imageURL)
		} else if hasEncodedImage {
			imageBytes, err = s.loadImageURLOrBase64(ctx, imageURL, b64Field)
		} else {
			imageBytes, err = s.downloadImageWithRetry(ctx, imageURL)
		}
	}
	if imageURL == "" || err != nil {
		var encodedImage string
		if hasEncodedImage {
			_ = json.Unmarshal(b64Field, &encodedImage)
		}
		if encodedImage != "" {
			imageBytes, err = decodeRelayImage(encodedImage)
		}
	}
	if err != nil {
		return nil, false, fmt.Errorf("%w: %v", ErrRelayImageDeliveryFailed, err)
	}
	persistStarted := time.Now()
	localURL, err := s.persist(imageBytes)
	if err != nil {
		common.SysLog(common.LogText("relay image persistence failed: image_bytes=%d elapsed=%s", len(imageBytes), time.Since(persistStarted).Round(time.Millisecond)))
		return nil, false, fmt.Errorf("%w: %v", ErrRelayImageDeliveryFailed, err)
	}
	if elapsed := time.Since(persistStarted); elapsed >= time.Second {
		common.SysLog(common.LogText("relay image persistence slow: image_bytes=%d elapsed=%s", len(imageBytes), elapsed.Round(time.Millisecond)))
	}
	encodedURL, _ := json.Marshal(localURL)
	fields["url"] = encodedURL
	delete(fields, "b64_json")
	if _, exists := fields["image_url"]; exists {
		fields["image_url"] = encodedURL
	}
	rewritten, err := json.Marshal(fields)
	if err != nil {
		return nil, false, fmt.Errorf("%w: %v", ErrRelayImageDeliveryFailed, err)
	}
	return rewritten, true, nil
}

// loadImageURLOrBase64 races the preferred URL against a delayed local Base64
// decode. URL is still preferred when it is fast, while a slow provider CDN no
// longer holds up a response that already contains the image bytes.
func (s *relayImageStore) loadImageURLOrBase64(ctx context.Context, imageURL string, b64Field json.RawMessage) ([]byte, error) {
	type imageLoadResult struct {
		bytes []byte
		err   error
	}
	resultCh := make(chan imageLoadResult, 2)
	raceCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		imageBytes, err := s.downloadImageWithRetry(raceCtx, imageURL)
		if err == nil {
			err = validateRelayImageBytes(imageBytes)
		}
		resultCh <- imageLoadResult{bytes: imageBytes, err: err}
	}()

	startBase64 := func() {
		var encodedImage string
		_ = common.Unmarshal(b64Field, &encodedImage)
		go func() {
			imageBytes, err := decodeRelayImage(encodedImage)
			if err == nil {
				err = validateRelayImageBytes(imageBytes)
			}
			resultCh <- imageLoadResult{bytes: imageBytes, err: err}
		}()
	}

	timer := time.NewTimer(relayImageBase64RaceDelay)
	defer timer.Stop()
	base64Started := false
	pending := 1
	var firstErr error
	for pending > 0 {
		select {
		case result := <-resultCh:
			pending--
			if result.err == nil {
				return result.bytes, nil
			}
			if firstErr == nil {
				firstErr = result.err
			}
			if !base64Started {
				base64Started = true
				pending++
				startBase64()
			}
		case <-timer.C:
			if !base64Started {
				base64Started = true
				pending++
				startBase64()
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, firstErr
}

func validateRelayImageBytes(imageBytes []byte) error {
	if len(imageBytes) == 0 {
		return errors.New("image is empty")
	}
	header := imageBytes
	if len(header) > 64<<10 {
		header = header[:64<<10]
	}
	_, err := validateHostedImageHeader(header)
	return err
}

func (s *relayImageStore) rewriteEventStream(ctx context.Context, body []byte) ([]byte, error) {
	output := make([]byte, 0, len(body)+1024)
	remaining := body
	for len(remaining) > 0 {
		lf := bytes.Index(remaining, []byte("\n\n"))
		crlf := bytes.Index(remaining, []byte("\r\n\r\n"))
		index, delimiterLength := lf, 2
		if crlf >= 0 && (lf < 0 || crlf < lf) {
			index, delimiterLength = crlf, 4
		}
		if index < 0 {
			event, err := s.rewriteEvent(ctx, remaining)
			if err != nil {
				return nil, err
			}
			output = append(output, event...)
			break
		}
		event, err := s.rewriteEvent(ctx, remaining[:index])
		if err != nil {
			return nil, err
		}
		output = append(output, event...)
		output = append(output, remaining[index:index+delimiterLength]...)
		remaining = remaining[index+delimiterLength:]
	}
	return output, nil
}

func (s *relayImageStore) rewriteEvent(ctx context.Context, event []byte) ([]byte, error) {
	lines := bytes.Split(event, []byte("\n"))
	var dataParts [][]byte
	firstData := -1
	for i, line := range lines {
		if bytes.HasPrefix(line, []byte("data:")) {
			if firstData < 0 {
				firstData = i
			}
			value := bytes.TrimPrefix(line, []byte("data:"))
			dataParts = append(dataParts, bytes.TrimPrefix(value, []byte(" ")))
		}
	}
	if firstData < 0 {
		return event, nil
	}
	data := bytes.Join(dataParts, []byte("\n"))
	if bytes.Equal(data, []byte("[DONE]")) {
		return event, nil
	}
	var eventType struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(data, &eventType)
	if strings.EqualFold(eventType.Type, "error") || strings.EqualFold(eventType.Type, "upstream_error") {
		if IsRelayImageContentRejected(http.StatusBadRequest, data) {
			return nil, ErrRelayImageRejected
		}
		return nil, ErrRelayImageGeneration
	}
	rewritten, err := s.rewriteJSON(ctx, data, true)
	if err != nil {
		return nil, err
	}
	if bytes.Equal(data, rewritten) {
		return event, nil
	}
	output := make([]byte, 0, len(event)+len(rewritten))
	for i, line := range lines {
		if i == firstData {
			output = append(output, []byte("data: ")...)
			output = append(output, rewritten...)
			output = append(output, '\n')
			continue
		}
		if bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		output = append(output, line...)
		if i < len(lines)-1 {
			output = append(output, '\n')
		}
	}
	return bytes.TrimSuffix(output, []byte("\n")), nil
}

func relayImageResponseFailure(payload map[string]json.RawMessage, body []byte) error {
	if raw, ok := payload["error"]; ok && string(raw) != "null" && string(raw) != "{}" && string(raw) != `""` {
		if IsRelayImageContentRejected(http.StatusBadRequest, raw) {
			return ErrRelayImageRejected
		}
		return ErrRelayImageGeneration
	}
	var responseType string
	_ = json.Unmarshal(payload["type"], &responseType)
	if strings.EqualFold(responseType, "error") || strings.EqualFold(responseType, "upstream_error") {
		if IsRelayImageContentRejected(http.StatusBadRequest, body) {
			return ErrRelayImageRejected
		}
		return ErrRelayImageGeneration
	}
	return nil
}

// IsRelayImageContentRejected recognizes policy/moderation codes without
// returning provider-supplied text to the caller.
func IsRelayImageContentRejected(statusCode int, body []byte) bool {
	if statusCode < http.StatusBadRequest || statusCode >= http.StatusInternalServerError {
		return false
	}
	return IsTaskContentRejection("", statusCode, body)
}

// RelayImageUpstreamErrorMessage exposes only a provider's human-readable
// reason, never its raw response body. URLs and common credential forms are
// removed before the message is returned to an image API client.
func RelayImageUpstreamErrorMessage(statusCode int, body []byte, upstreamURLs ...string) string {
	var response struct {
		Error    json.RawMessage `json:"error"`
		Message  string          `json:"message"`
		Msg      string          `json:"msg"`
		Err      string          `json:"err"`
		ErrorMsg string          `json:"error_msg"`
		Detail   string          `json:"detail"`
		Header   struct {
			Message string `json:"message"`
		} `json:"header"`
		Response struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		} `json:"response"`
	}

	if common.Unmarshal(body, &response) == nil {
		message := response.Message
		if message == "" && len(response.Error) > 0 {
			switch common.GetJsonType(response.Error) {
			case "object":
				var nested struct {
					Message string `json:"message"`
				}
				_ = common.Unmarshal(response.Error, &nested)
				message = nested.Message
			case "string":
				_ = common.Unmarshal(response.Error, &message)
			}
		}
		if message == "" {
			message = response.Msg
		}
		if message == "" {
			message = response.Err
		}
		if message == "" {
			message = response.ErrorMsg
		}
		if message == "" {
			message = response.Detail
		}
		if message == "" {
			message = response.Header.Message
		}
		if message == "" {
			message = response.Response.Error.Message
		}
		if message != "" {
			return sanitizeRelayImageErrorMessage(message, upstreamURLs...)
		}
	}

	if statusCode >= http.StatusBadRequest && statusCode <= 599 {
		return fmt.Sprintf("图片生成失败（HTTP %d）", statusCode)
	}
	return "图片生成失败，请稍后重试"
}

// SanitizeRelayImageErrorMessage is also used for already-extracted error
// messages stored on asynchronous task records.
func SanitizeRelayImageErrorMessage(message string, upstreamURLs ...string) string {
	return sanitizeRelayImageErrorMessage(message, upstreamURLs...)
}

func sanitizeRelayImageErrorMessage(message string, upstreamURLs ...string) string {
	message = common.MaskSensitiveInfo(message, upstreamURLs...)
	message = relayImageErrorURLPattern.ReplaceAllString(message, "[链接已隐藏]")
	message = relayImageBearerPattern.ReplaceAllString(message, "Bearer ***")
	message = relayImageAPIKeyPattern.ReplaceAllString(message, "api_key=***")
	message = relayImageSecretKeyPattern.ReplaceAllString(message, "sk-***")
	message = strings.Join(strings.Fields(message), " ")
	message = strings.TrimSpace(message)
	if len([]rune(message)) > 512 {
		message = string([]rune(message)[:512])
	}
	return message
}

// IsRelayImagePreAcceptanceConnectionError limits image-generation retries to
// transport failures that occur before an HTTP response is received.
func IsRelayImagePreAcceptanceConnectionError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var operationErr *net.OpError
	return errors.As(err, &operationErr) && operationErr.Op == "dial" && operationErr.Timeout()
}

func IsRelayImageJSONContentType(contentType string) bool {
	return isJSONContentType(contentType)
}

func decodeRelayImage(encoded string) ([]byte, error) {
	return decodeRelayImageLimit(encoded, relayImageMaxBytes)
}

func decodeRelayImageLimit(encoded string, maximum int64) ([]byte, error) {
	if strings.HasPrefix(strings.ToLower(encoded), "data:") {
		header, data, found := strings.Cut(encoded, ",")
		if !found || !strings.HasSuffix(strings.ToLower(header), ";base64") {
			return nil, errors.New("invalid image data URL")
		}
		encoded = data
	}
	if int64(len(encoded)) > (maximum+2)/3*4+8 {
		return nil, errors.New("encoded image exceeds size limit")
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(encoded)
	}
	if err != nil {
		decoded, err = base64.RawURLEncoding.DecodeString(encoded)
	}
	if err != nil {
		return nil, errors.New("invalid base64 image data")
	}
	if int64(len(decoded)) > maximum {
		return nil, errors.New("image exceeds size limit")
	}
	return decoded, nil
}

func (s *relayImageStore) downloadImageWithRetry(ctx context.Context, imageURL string) ([]byte, error) {
	return s.downloadImageWithRetryLimit(ctx, imageURL, relayImageMaxBytes)
}

func (s *relayImageStore) downloadImageWithRetryLimit(ctx context.Context, imageURL string, maximum int64) ([]byte, error) {
	backoff := [...]time.Duration{300 * time.Millisecond, 900 * time.Millisecond}
	downloadCtx, cancel := context.WithTimeout(ctx, relayImageDownloadTotalTimeout)
	defer cancel()
	started := time.Now()
	attempts := 0
	var lastErr error
	for attempt := range len(backoff) + 1 {
		if err := downloadCtx.Err(); err != nil {
			return nil, err
		}
		if attempt > 0 {
			timer := time.NewTimer(backoff[attempt-1])
			select {
			case <-downloadCtx.Done():
				timer.Stop()
				return nil, downloadCtx.Err()
			case <-timer.C:
			}
		}
		attempts = attempt + 1
		attemptCtx, attemptCancel := context.WithTimeout(downloadCtx, relayImageDownloadAttemptTimeout)
		imageBytes, err := s.downloadImageLimit(attemptCtx, imageURL, maximum)
		attemptCancel()
		if err == nil {
			if elapsed := time.Since(started); elapsed >= 3*time.Second {
				common.SysLog(common.LogText("relay image download slow: attempts=%d elapsed=%s", attempts, elapsed.Round(time.Millisecond)))
			}
			return imageBytes, nil
		}
		lastErr = err
		if downloadCtx.Err() != nil {
			lastErr = downloadCtx.Err()
			common.SysLog(common.LogText("relay image download stopped: attempts=%d failure=%s elapsed=%s", attempts, relayImageDownloadFailureKind(lastErr), time.Since(started).Round(time.Millisecond)))
			return nil, lastErr
		}
		if !isRetryableRelayImageDownloadError(err) {
			common.SysLog(common.LogText("relay image download stopped: attempts=%d failure=%s elapsed=%s", attempts, relayImageDownloadFailureKind(err), time.Since(started).Round(time.Millisecond)))
			return nil, err
		}
	}
	common.SysLog(common.LogText("relay image download failed: attempts=%d failure=%s elapsed=%s", attempts, relayImageDownloadFailureKind(lastErr), time.Since(started).Round(time.Millisecond)))
	return nil, lastErr
}

type relayImageDownloadStatusError struct {
	statusCode int
}

func (e *relayImageDownloadStatusError) Error() string {
	return fmt.Sprintf("image download returned status %d", e.statusCode)
}

func isRetryableRelayImageDownloadError(err error) bool {
	var statusErr *relayImageDownloadStatusError
	if errors.As(err, &statusErr) {
		return statusErr.statusCode == http.StatusRequestTimeout || statusErr.statusCode == http.StatusTooManyRequests || statusErr.statusCode >= http.StatusInternalServerError
	}
	var networkErr net.Error
	return errors.As(err, &networkErr) || errors.Is(err, io.ErrUnexpectedEOF)
}

func relayImageDownloadFailureKind(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "truncated_body"
	}
	var statusErr *relayImageDownloadStatusError
	if errors.As(err, &statusErr) {
		return fmt.Sprintf("http_%d", statusErr.statusCode)
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) {
		return "network"
	}
	return "invalid_response"
}

func (s *relayImageStore) downloadImage(ctx context.Context, imageURL string) ([]byte, error) {
	return s.downloadImageLimit(ctx, imageURL, relayImageMaxBytes)
}

func (s *relayImageStore) downloadImageLimit(ctx context.Context, imageURL string, maximum int64) ([]byte, error) {
	parsed, err := url.Parse(strings.TrimSpace(imageURL))
	if err != nil || parsed == nil || parsed.Host == "" || parsed.User != nil ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, errors.New("image URL is invalid")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return nil, err
	}
	response, err := s.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, &relayImageDownloadStatusError{statusCode: response.StatusCode}
	}
	if response.ContentLength > maximum {
		return nil, errors.New("downloaded image exceeds size limit")
	}
	imageBytes, err := io.ReadAll(io.LimitReader(response.Body, maximum+1))
	if err != nil {
		return nil, err
	}
	if len(imageBytes) == 0 || int64(len(imageBytes)) > maximum {
		return nil, errors.New("downloaded image exceeds size limit or is empty")
	}
	return imageBytes, nil
}

func (s *relayImageStore) persist(imageBytes []byte) (string, error) {
	return s.persistWithPolicy(imageBytes, relayImageArtifactKey, relayImageRetention, "")
}

func (s *relayImageStore) persistWithPolicy(imageBytes []byte, artifactKey string, retention time.Duration, idPrefix string) (string, error) {
	if s.root == "" || s.baseAddress == "" {
		return "", ErrTaskArtifactStoreDisabled
	}
	maximum := relayImageMaxBytes
	if artifactKey == relayImageReferenceKey {
		maximum = relayImageReferenceMaxBytes
	}
	if len(imageBytes) == 0 || int64(len(imageBytes)) > maximum {
		return "", errors.New("image exceeds size limit")
	}
	header := imageBytes
	if len(header) > 64<<10 {
		header = header[:64<<10]
	}
	if _, err := validateHostedImageHeader(header); err != nil {
		return "", err
	}
	if err := ValidateTaskArtifactBaseURL(s.baseAddress); err != nil {
		return "", err
	}
	var randomID [16]byte
	if _, err := rand.Read(randomID[:]); err != nil {
		return "", err
	}
	id := idPrefix + base64.RawURLEncoding.EncodeToString(randomID[:])
	if err := os.MkdirAll(s.root, 0o750); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(s.root, ".image-*")
	if err != nil {
		return "", err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o640); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if _, err := tmp.Write(imageBytes); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	filePath := s.imagePath(id)
	if err := os.Rename(tmpPath, filePath); err != nil {
		return "", err
	}
	expiresAt := time.Now().Add(retention)
	access, err := IssueTaskArtifactAccessWithExpiry(id, artifactKey, expiresAt)
	if err != nil {
		_ = os.Remove(filePath)
		return "", err
	}
	baseURL, err := url.Parse(s.baseAddress)
	if err != nil {
		_ = os.Remove(filePath)
		return "", err
	}
	basePath := strings.TrimRight(baseURL.Path, "/")
	escapedBasePath := strings.TrimRight(baseURL.EscapedPath(), "/")
	suffix := "/v1/images/relay/" + id + "/" + artifactKey + "/content"
	baseURL.Path = basePath + suffix
	baseURL.RawPath = escapedBasePath + suffix
	query := baseURL.Query()
	query.Set(TaskArtifactAccessQueryParameter, access)
	baseURL.RawQuery = query.Encode()
	return baseURL.String(), nil
}

func (s *relayImageStore) imagePath(id string) string {
	return filepath.Join(s.root, id+".bin")
}

// ServeRelayImageContent serves a previously stored image. The router checks
// the signed capability before reaching this handler.
func ServeRelayImageContent(c *gin.Context, id, artifactKey string) error {
	store := getRelayImageStore()
	if store == nil || store.root == "" {
		return ErrTaskArtifactStoreDisabled
	}
	return store.Serve(c, id, artifactKey)
}

func (s *relayImageStore) Serve(c *gin.Context, id, artifactKey string) error {
	retention, valid := relayImageRetentionFor(id, artifactKey)
	if !valid || !validRelayImageID(id) {
		return os.ErrNotExist
	}
	path := s.imagePath(id)
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if time.Since(info.ModTime()) >= retention {
		_ = os.Remove(path)
		return os.ErrNotExist
	}
	header := make([]byte, min(info.Size(), 64<<10))
	if len(header) == 0 {
		return os.ErrNotExist
	}
	n, readErr := file.Read(header)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return readErr
	}
	mimeType, err := validateHostedImageHeader(header[:n])
	if err != nil {
		return os.ErrNotExist
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	c.Header("Cache-Control", hostedArtifactCacheControl(info.ModTime().Add(retention)))
	c.Header("X-Content-Type-Options", "nosniff")
	c.Header("Content-Type", mimeType)
	c.Header("Content-Disposition", "inline")
	http.ServeContent(c.Writer, c.Request, id, info.ModTime(), file)
	return nil
}

func validRelayImageID(id string) bool {
	encodedID := strings.TrimPrefix(id, relayImageReferencePrefix)
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(encodedID)
	return err == nil && len(decoded) == 16
}

func relayImageRetentionFor(id, artifactKey string) (time.Duration, bool) {
	if artifactKey == relayImageReferenceKey && strings.HasPrefix(id, relayImageReferencePrefix) {
		return relayImageReferenceRetention, true
	}
	if artifactKey == relayImageArtifactKey && !strings.HasPrefix(id, relayImageReferencePrefix) {
		return relayImageRetention, true
	}
	return 0, false
}

func (s *relayImageStore) cleanupLoop() {
	s.cleanupExpired(time.Now())
	ticker := time.NewTicker(15 * time.Minute)
	defer ticker.Stop()
	for now := range ticker.C {
		s.cleanupExpired(now)
	}
}

func (s *relayImageStore) cleanupExpired(now time.Time) {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".bin") {
			continue
		}
		info, err := entry.Info()
		if err == nil {
			id := strings.TrimSuffix(entry.Name(), ".bin")
			artifactKey := relayImageArtifactKey
			if strings.HasPrefix(id, relayImageReferencePrefix) {
				artifactKey = relayImageReferenceKey
			}
			retention, valid := relayImageRetentionFor(id, artifactKey)
			if valid && now.Sub(info.ModTime()) >= retention {
				_ = os.Remove(filepath.Join(s.root, entry.Name()))
			}
		}
	}
}

func newRelayImageHTTPClient() *http.Client {
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           dialPublicImageHost,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		MaxIdleConns:          16,
		IdleConnTimeout:       30 * time.Second,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   25 * time.Second,
		CheckRedirect: func(request *http.Request, via []*http.Request) error {
			if len(via) >= 3 || request.URL.User != nil || (request.URL.Scheme != "http" && request.URL.Scheme != "https") {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}
}

func dialPublicImageHost(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if strings.EqualFold(strings.TrimSuffix(host, "."), "localhost") || strings.HasSuffix(strings.ToLower(host), ".localhost") {
		return nil, errors.New("private image host is not allowed")
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil || len(ips) == 0 {
		if err == nil {
			err = errors.New("image host has no addresses")
		}
		return nil, err
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	var lastErr error
	for _, addr := range ips {
		ip := addr.IP
		if !isPublicImageIP(ip) {
			return nil, errors.New("private image host is not allowed")
		}
		connection, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if dialErr == nil {
			return connection, nil
		}
		lastErr = dialErr
	}
	if lastErr == nil {
		lastErr = syscall.ECONNREFUSED
	}
	return nil, lastErr
}

func isPublicImageIP(ip net.IP) bool {
	return ip != nil && ip.IsGlobalUnicast() && !ip.IsPrivate() && !ip.IsLoopback() &&
		!ip.IsLinkLocalUnicast() && !ip.IsLinkLocalMulticast() && !ip.IsMulticast() && !ip.IsUnspecified()
}
