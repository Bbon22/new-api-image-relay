package service

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/system_setting"
)

const (
	TaskArtifactAccessQueryParameter = "access"
	taskArtifactAccessVersion        = "v1"
	taskArtifactAccessLength         = 43
	taskArtifactAccessSignedVersion  = "v2"
	taskArtifactAccessMaxTTL         = 9 * 60 * 60
	maxTaskArtifactTaskIDLength      = 191
	maxTaskArtifactKeyLength         = 128
)

var ErrTaskArtifactAccessInvalid = errors.New("task artifact access is invalid")

func signedTaskArtifactAccessMessage(version, expiresAt, taskID, artifactKey string) []byte {
	return []byte(taskArtifactAccessSignedVersion + "\x00" + version + "\x00" + expiresAt + "\x00" + taskID + "\x00" + artifactKey)
}

// IssueTaskArtifactAccess preserves the compact legacy capability format for
// internal callers. Customer-facing URLs use the versioned expiring format
// emitted by IssueTaskArtifactAccessWithExpiry.
func IssueTaskArtifactAccess(taskID, artifactKey string) (string, error) {
	taskID = strings.TrimSpace(taskID)
	artifactKey = strings.TrimSpace(artifactKey)
	if taskID == "" || len(taskID) > maxTaskArtifactTaskIDLength ||
		artifactKey == "" || len(artifactKey) > maxTaskArtifactKeyLength || common.CryptoSecret == "" {
		return "", ErrTaskArtifactAccessInvalid
	}
	mac := hmac.New(sha256.New, []byte(common.CryptoSecret))
	_, _ = mac.Write([]byte(taskArtifactAccessVersion + "\x00" + taskID + "\x00" + artifactKey))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

// VerifyTaskArtifactAccess verifies the route binding without reading task,
// user, or token state. Signature comparison is constant-time.
func VerifyTaskArtifactAccess(access, taskID, artifactKey string) bool {
	taskID = strings.TrimSpace(taskID)
	artifactKey = strings.TrimSpace(artifactKey)
	if taskID == "" || len(taskID) > maxTaskArtifactTaskIDLength ||
		artifactKey == "" || len(artifactKey) > maxTaskArtifactKeyLength {
		return false
	}
	if verifySignedTaskArtifactAccess(access, taskID, artifactKey) {
		return true
	}
	if len(access) != taskArtifactAccessLength || common.CryptoSecret == "" {
		return false
	}
	actualSignature, err := base64.RawURLEncoding.Strict().DecodeString(access)
	if err != nil || len(actualSignature) != sha256.Size {
		return false
	}
	mac := hmac.New(sha256.New, []byte(common.CryptoSecret))
	_, _ = mac.Write([]byte(taskArtifactAccessVersion + "\x00" + taskID + "\x00" + artifactKey))
	return hmac.Equal(actualSignature, mac.Sum(nil))
}

// IssueTaskArtifactAccessWithExpiry emits the versioned, expiring capability
// used in customer-facing URLs.
func IssueTaskArtifactAccessWithExpiry(taskID, artifactKey string, expiresAt time.Time) (string, error) {
	taskID = strings.TrimSpace(taskID)
	artifactKey = strings.TrimSpace(artifactKey)
	if taskID == "" || len(taskID) > maxTaskArtifactTaskIDLength ||
		artifactKey == "" || len(artifactKey) > maxTaskArtifactKeyLength ||
		common.CryptoSecret == "" {
		return "", ErrTaskArtifactAccessInvalid
	}
	now := time.Now()
	if expiresAt.Before(now) || expiresAt.After(now.Add(taskArtifactAccessMaxTTL*time.Second)) {
		return "", ErrTaskArtifactAccessInvalid
	}
	version := common.GetEnvOrDefaultString("TASK_ARTIFACT_SIGNING_KEY_VERSION", "1")
	expires := strconv.FormatInt(expiresAt.Unix(), 10)
	mac := hmac.New(sha256.New, []byte(common.CryptoSecret))
	_, _ = mac.Write(signedTaskArtifactAccessMessage(version, expires, taskID, artifactKey))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return strings.Join([]string{taskArtifactAccessSignedVersion, version, expires, signature}, "."), nil
}

func verifySignedTaskArtifactAccess(access, taskID, artifactKey string) bool {
	parts := strings.Split(access, ".")
	if len(parts) != 4 || parts[0] != taskArtifactAccessSignedVersion ||
		parts[1] == "" || len(parts[1]) > 32 || len(parts[2]) > 16 || len(parts[3]) != taskArtifactAccessLength {
		return false
	}
	expiresUnix, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || expiresUnix < time.Now().Unix() {
		return false
	}
	actual, err := base64.RawURLEncoding.Strict().DecodeString(parts[3])
	if err != nil || len(actual) != sha256.Size {
		return false
	}
	secret := ""
	currentVersion := common.GetEnvOrDefaultString("TASK_ARTIFACT_SIGNING_KEY_VERSION", "1")
	if parts[1] == currentVersion {
		secret = common.CryptoSecret
	} else if parts[1] == strings.TrimSpace(common.GetEnvOrDefaultString("TASK_ARTIFACT_SIGNING_OLD_VERSION", "")) {
		oldValidUntil, parseErr := strconv.ParseInt(common.GetEnvOrDefaultString("TASK_ARTIFACT_SIGNING_OLD_VALID_UNTIL", "0"), 10, 64)
		if parseErr == nil && time.Now().Unix() <= oldValidUntil {
			secret = strings.TrimSpace(common.GetEnvOrDefaultString("TASK_ARTIFACT_SIGNING_OLD_SECRET", ""))
		}
	}
	if secret == "" {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(signedTaskArtifactAccessMessage(parts[1], parts[2], taskID, artifactKey))
	return hmac.Equal(actual, mac.Sum(nil))
}

// ValidateTaskArtifactBaseURL validates configuration syntax only. It
// deliberately performs no DNS lookup or reachability probe.
func ValidateTaskArtifactBaseURL(raw string) error {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return errors.New("task artifact base URL is empty")
	}
	if raw != trimmed {
		return errors.New("task artifact base URL must not contain surrounding whitespace")
	}
	raw = trimmed
	parsed, err := url.Parse(raw)
	if err != nil || parsed == nil {
		return errors.New("task artifact base URL is invalid")
	}
	if !strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https") {
		return errors.New("task artifact base URL must use http or https")
	}
	if parsed.Host == "" || parsed.User != nil || parsed.Opaque != "" {
		return errors.New("task artifact base URL must contain a host and no userinfo")
	}
	if parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || strings.Contains(raw, "#") {
		return errors.New("task artifact base URL must not contain a query or fragment")
	}
	return nil
}

// BuildTaskArtifactContentURL returns an absolute, long-lived capability URL.
// TaskPublicAddress wins when configured; ServerAddress is the only fallback.
// Request Host headers are intentionally not involved.
func BuildTaskArtifactContentURL(taskID, artifactKey string, resultExpiresAt ...int64) (string, error) {
	taskID = strings.TrimSpace(taskID)
	artifactKey = strings.TrimSpace(artifactKey)
	if taskID == "" || len(taskID) > maxTaskArtifactTaskIDLength ||
		artifactKey == "" || len(artifactKey) > maxTaskArtifactKeyLength {
		return "", ErrTaskArtifactAccessInvalid
	}

	baseAddress := strings.TrimSpace(system_setting.TaskPublicAddress)
	if baseAddress == "" {
		baseAddress = strings.TrimSpace(common.GetEnvOrDefaultString("TASK_PUBLIC_ADDRESS", ""))
	}
	if baseAddress == "" {
		baseAddress = strings.TrimSpace(system_setting.ServerAddress)
	}
	if err := ValidateTaskArtifactBaseURL(baseAddress); err != nil {
		return "", err
	}
	baseURL, err := url.Parse(baseAddress)
	if err != nil {
		return "", err
	}

	expiresAt := time.Now().Add(time.Duration(model.TaskResultRetentionSeconds) * time.Second)
	if len(resultExpiresAt) > 0 && resultExpiresAt[0] > 0 {
		expiresAt = minTime(expiresAt, time.Unix(resultExpiresAt[0], 0))
	}
	access, err := IssueTaskArtifactAccessWithExpiry(taskID, artifactKey, expiresAt)
	if err != nil {
		return "", err
	}

	basePath := strings.TrimRight(baseURL.Path, "/")
	escapedBasePath := strings.TrimRight(baseURL.EscapedPath(), "/")
	suffixPath := fmt.Sprintf("/v1/tasks/%s/artifacts/%s/content", taskID, artifactKey)
	escapedSuffixPath := fmt.Sprintf(
		"/v1/tasks/%s/artifacts/%s/content",
		url.PathEscape(taskID),
		url.PathEscape(artifactKey),
	)
	baseURL.Path = basePath + suffixPath
	baseURL.RawPath = escapedBasePath + escapedSuffixPath
	query := baseURL.Query()
	query.Set(TaskArtifactAccessQueryParameter, access)
	baseURL.RawQuery = query.Encode()
	return baseURL.String(), nil
}

func minTime(left, right time.Time) time.Time {
	if right.Before(left) {
		return right
	}
	return left
}
