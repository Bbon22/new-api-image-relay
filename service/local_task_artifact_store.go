package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/types"
	"github.com/gin-gonic/gin"
	"golang.org/x/image/webp"
)

const localArtifactMaxBytes int64 = 64 << 20

type localArtifactStore struct{ root string }

func newLocalArtifactStore(root string) TaskArtifactStore {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return &localArtifactStore{}
	}
	return &localArtifactStore{root: filepath.Clean(absRoot)}
}

func (s *localArtifactStore) Enabled() bool { return s != nil && s.root != "" }

func (s *localArtifactStore) artifactPath(taskID, artifactKey string) string {
	return filepath.Join(s.root, taskID, artifactKey+".bin")
}

func (s *localArtifactStore) metadataPath(taskID, artifactKey string) string {
	return filepath.Join(s.root, taskID, artifactKey+".meta")
}

func (s *localArtifactStore) Resolve(task *model.Task, artifactKey string) (*StoredArtifactRef, error) {
	if !s.Enabled() || task == nil || strings.TrimSpace(task.TaskID) == "" {
		return nil, nil
	}
	if !validArtifactSegment(task.TaskID) || !validArtifactSegment(artifactKey) {
		return nil, errors.New("invalid artifact path")
	}
	if task.PrivateData.ResultExpiresAt > 0 && time.Now().Unix() >= task.PrivateData.ResultExpiresAt {
		return nil, nil
	}
	path := s.artifactPath(task.TaskID, artifactKey)
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	mimeType := "application/octet-stream"
	if metadata, readErr := os.ReadFile(s.metadataPath(task.TaskID, artifactKey)); readErr == nil {
		mimeType = strings.TrimSpace(string(metadata))
	}
	return &StoredArtifactRef{Backend: "local", ObjectKey: path, MimeType: mimeType, Size: info.Size(), ExpiresAt: task.PrivateData.ResultExpiresAt}, nil
}

func (s *localArtifactStore) Persist(ctx context.Context, task *model.Task, artifact types.TaskArtifact, content io.Reader) (*StoredArtifactRef, error) {
	if !s.Enabled() {
		return nil, ErrTaskArtifactStoreDisabled
	}
	if task == nil || task.TaskID == "" || artifact.Key == "" || content == nil {
		return nil, errors.New("invalid artifact persistence request")
	}
	if !validArtifactSegment(task.TaskID) || !validArtifactSegment(artifact.Key) {
		return nil, errors.New("invalid artifact path")
	}
	if task.PrivateData.ResultExpiresAt > 0 && time.Now().Unix() >= task.PrivateData.ResultExpiresAt {
		return nil, os.ErrNotExist
	}
	mimeType := strings.TrimSpace(artifact.MimeType)
	if artifact.Type == "image" {
		header, err := io.ReadAll(io.LimitReader(content, 64<<10))
		if err != nil {
			return nil, err
		}
		mimeType, err = validateHostedImageHeader(header)
		if err != nil {
			return nil, err
		}
		content = io.MultiReader(bytes.NewReader(header), content)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(s.root, task.TaskID), 0o750); err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(filepath.Join(s.root, task.TaskID), ".artifact-*")
	if err != nil {
		return nil, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	written, copyErr := io.Copy(tmp, io.LimitReader(content, localArtifactMaxBytes+1))
	if closeErr := tmp.Close(); copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		return nil, copyErr
	}
	if written <= 0 || written > localArtifactMaxBytes {
		return nil, fmt.Errorf("artifact exceeds %d bytes", localArtifactMaxBytes)
	}
	path := s.artifactPath(task.TaskID, artifact.Key)
	if err := os.Rename(tmpName, path); err != nil {
		return nil, err
	}
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	if err := os.WriteFile(s.metadataPath(task.TaskID, artifact.Key), []byte(mimeType), 0o640); err != nil {
		return nil, err
	}
	if task.PrivateData.ImageTask && task.PrivateData.ResultExpiresAt == 0 {
		task.PrivateData.ResultExpiresAt = time.Now().Unix() + model.TaskResultRetentionSeconds
	}
	if task.PrivateData.ImageTask && task.PrivateData.SettlementStatus == "" {
		task.PrivateData.SettlementStatus = "succeeded"
	}
	if task.PrivateData.ImageTask && artifact.Type == "image" {
		task.PrivateData.MediaDeliveryStatus = "succeeded"
	}
	if !slices.Contains(task.PrivateData.HostedImageKeys, artifact.Key) && task.PrivateData.ImageTask {
		task.PrivateData.HostedImageKeys = append(task.PrivateData.HostedImageKeys, artifact.Key)
	}
	if err := task.UpdatePrivateData(); err != nil {
		return nil, err
	}
	return s.Resolve(task, artifact.Key)
}

func validateHostedImageHeader(header []byte) (string, error) {
	config, format, err := image.DecodeConfig(bytes.NewReader(header))
	if err != nil {
		config, err = webp.DecodeConfig(bytes.NewReader(header))
		format = "webp"
	}
	if err != nil {
		return "", fmt.Errorf("invalid hosted image: %w", err)
	}
	if config.Width <= 0 || config.Height <= 0 || config.Width > 8192 || config.Height > 8192 || int64(config.Width)*int64(config.Height) > 67_000_000 {
		return "", errors.New("hosted image dimensions exceed the allowed limit")
	}
	switch format {
	case "jpeg":
		return "image/jpeg", nil
	case "png":
		return "image/png", nil
	case "webp":
		return "image/webp", nil
	default:
		return "", errors.New("hosted image must be JPG, PNG, or WebP")
	}
}

func (s *localArtifactStore) Serve(c *gin.Context, task *model.Task, ref *StoredArtifactRef) error {
	if ref == nil || ref.Backend != "local" || ref.ObjectKey == "" {
		return ErrTaskArtifactStoreDisabled
	}
	if task == nil || task.PrivateData.ResultExpiresAt > 0 && time.Now().Unix() >= task.PrivateData.ResultExpiresAt {
		return os.ErrNotExist
	}
	file, err := os.Open(ref.ObjectKey)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	expiresAt := time.Time{}
	if task.PrivateData.ResultExpiresAt > 0 {
		expiresAt = time.Unix(task.PrivateData.ResultExpiresAt, 0)
	}
	c.Header("Cache-Control", hostedArtifactCacheControl(expiresAt))
	c.Header("Content-Type", ref.MimeType)
	http.ServeContent(c.Writer, c.Request, filepath.Base(ref.ObjectKey), info.ModTime(), file)
	return nil
}

func (s *localArtifactStore) Delete(task *model.Task) error {
	if !s.Enabled() || task == nil || !validArtifactSegment(task.TaskID) {
		return nil
	}
	return os.RemoveAll(filepath.Join(s.root, task.TaskID))
}

func validArtifactSegment(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && value != "." && value != ".." &&
		!strings.ContainsAny(value, "/\\\x00\r\n") && filepath.Base(value) == value
}

var _ TaskArtifactStore = (*localArtifactStore)(nil)
