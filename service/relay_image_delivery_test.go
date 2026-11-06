package service

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"image"
	"image/color"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRelayImageDeliveryRewritesURLToExpiringLocalArtifact(t *testing.T) {
	previousSecret := common.CryptoSecret
	common.CryptoSecret = "relay-image-delivery-test-secret"
	t.Cleanup(func() { common.CryptoSecret = previousSecret })
	gin.SetMode(gin.TestMode)

	imageBytes := testRelayPNG(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(imageBytes)
	}))
	defer upstream.Close()

	store := newRelayImageStore(t.TempDir(), "https://zmw.bon2.space", upstream.Client())
	body, err := json.Marshal(map[string]any{"data": []any{map[string]any{"url": upstream.URL + "/generated.png"}}})
	require.NoError(t, err)
	rewritten, err := store.RewriteResponse(context.Background(), body, "application/json")
	require.NoError(t, err)
	assert.NotContains(t, string(rewritten), upstream.URL)

	var result struct {
		Data []struct {
			URL string `json:"url"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rewritten, &result))
	require.Len(t, result.Data, 1)
	parsed, err := url.Parse(result.Data[0].URL)
	require.NoError(t, err)
	assert.Equal(t, "zmw.bon2.space", parsed.Host)
	assert.Contains(t, parsed.Path, "/v1/images/relay/")

	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	require.Len(t, parts, 6)
	id := parts[3]
	assert.True(t, VerifyTaskArtifactAccess(parsed.Query().Get(TaskArtifactAccessQueryParameter), id, relayImageArtifactKey))

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, parsed.RequestURI(), nil)
	require.NoError(t, store.Serve(c, id, relayImageArtifactKey))
	assert.Equal(t, "image/png", recorder.Header().Get("Content-Type"))
	assert.Contains(t, recorder.Header().Get("Cache-Control"), "private, max-age=")
	assert.NotContains(t, recorder.Header().Get("Cache-Control"), "no-store")
	assert.Equal(t, imageBytes, recorder.Body.Bytes())

	old := time.Now().Add(-relayImageRetention - time.Minute)
	path := filepath.Join(store.root, id+".bin")
	require.NoError(t, os.Chtimes(path, old, old))
	store.cleanupExpired(time.Now())
	_, err = os.Stat(path)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestRelayImageReferenceUsesReferenceCapabilityAndRetention(t *testing.T) {
	previousSecret := common.CryptoSecret
	common.CryptoSecret = "relay-image-reference-test-secret"
	t.Cleanup(func() { common.CryptoSecret = previousSecret })
	gin.SetMode(gin.TestMode)

	imageBytes := testRelayPNG(t)
	store := newRelayImageStore(t.TempDir(), "https://zmw.bon2.space", nil)
	referenceURL, err := store.persistWithPolicy(imageBytes, relayImageReferenceKey, relayImageReferenceRetention, relayImageReferencePrefix)
	require.NoError(t, err)
	parsed, err := url.Parse(referenceURL)
	require.NoError(t, err)
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	require.Len(t, parts, 6)
	id := parts[3]
	assert.True(t, strings.HasPrefix(id, relayImageReferencePrefix))
	assert.True(t, VerifyTaskArtifactAccess(parsed.Query().Get(TaskArtifactAccessQueryParameter), id, relayImageReferenceKey))
	assert.False(t, VerifyTaskArtifactAccess(parsed.Query().Get(TaskArtifactAccessQueryParameter), id, relayImageArtifactKey))

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, parsed.RequestURI(), nil)
	require.NoError(t, store.Serve(c, id, relayImageReferenceKey))
	assert.Equal(t, "image/png", recorder.Header().Get("Content-Type"))
	assert.Equal(t, imageBytes, recorder.Body.Bytes())

	old := time.Now().Add(-relayImageReferenceRetention - time.Minute)
	path := filepath.Join(store.root, id+".bin")
	require.NoError(t, os.Chtimes(path, old, old))
	store.cleanupExpired(time.Now())
	_, err = os.Stat(path)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestRelayImageDeliveryPrefersURLOverBase64(t *testing.T) {
	imageBytes := testRelayPNG(t)
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(imageBytes)
	}))
	defer upstream.Close()

	store := newRelayImageStore(t.TempDir(), "https://zmw.bon2.space", upstream.Client())
	body := []byte(`{"data":[{"url":"` + upstream.URL + `/generated.png","b64_json":"not-valid-base64"}]}`)
	rewritten, err := store.RewriteResponse(context.Background(), body, "application/json")
	require.NoError(t, err)
	assert.EqualValues(t, 1, requests.Load())
	assert.NotContains(t, string(rewritten), "b64_json")
}

func TestRelayImageDeliveryFallsBackToBase64WhenURLIsSlow(t *testing.T) {
	imageBytes := testRelayPNG(t)
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		time.Sleep(1500 * time.Millisecond)
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(imageBytes)
	}))
	defer upstream.Close()

	store := newRelayImageStore(t.TempDir(), "https://zmw.bon2.space", upstream.Client())
	encoded := base64.StdEncoding.EncodeToString(imageBytes)
	body := []byte(`{"data":[{"url":"` + upstream.URL + `/generated.png","b64_json":"` + encoded + `"}]}`)
	started := time.Now()
	rewritten, err := store.RewriteResponse(context.Background(), body, "application/json")
	elapsed := time.Since(started)
	require.NoError(t, err)
	assert.Less(t, elapsed, time.Second)
	assert.NotContains(t, string(rewritten), "b64_json")
	assert.EqualValues(t, 1, requests.Load())
}

func TestRelayImageDeliveryIgnoresInvalidURLWhenBase64IsValid(t *testing.T) {
	imageBytes := testRelayPNG(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write([]byte("not an image"))
	}))
	defer upstream.Close()

	store := newRelayImageStore(t.TempDir(), "https://zmw.bon2.space", upstream.Client())
	encoded := base64.StdEncoding.EncodeToString(imageBytes)
	body := []byte(`{"data":[{"url":"` + upstream.URL + `/invalid.png","b64_json":"` + encoded + `"}]}`)
	rewritten, err := store.RewriteResponse(context.Background(), body, "application/json")
	require.NoError(t, err)
	assert.NotContains(t, string(rewritten), upstream.URL)
	assert.NotContains(t, string(rewritten), "b64_json")
}

func TestRelayImageDeliveryFallsBackToBase64WhenURLFails(t *testing.T) {
	imageBytes := testRelayPNG(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporary download error", http.StatusBadGateway)
	}))
	defer upstream.Close()

	store := newRelayImageStore(t.TempDir(), "https://zmw.bon2.space", upstream.Client())
	encoded := base64.StdEncoding.EncodeToString(imageBytes)
	body := []byte(`{"data":[{"url":"` + upstream.URL + `/generated.png","b64_json":"` + encoded + `"}]}`)
	rewritten, err := store.RewriteResponse(context.Background(), body, "application/json")
	require.NoError(t, err)
	assert.NotContains(t, string(rewritten), upstream.URL)
	assert.NotContains(t, string(rewritten), "b64_json")
}

func TestRelayImageDeliveryFailureNeverReturnsUpstreamURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream temporary download error", http.StatusBadGateway)
	}))
	defer server.Close()
	store := newRelayImageStore(t.TempDir(), "https://zmw.bon2.space", server.Client())

	body := []byte(`{"data":[{"url":"` + server.URL + `/image.png"}]}`)
	rewritten, err := store.RewriteResponse(context.Background(), body, "application/json")
	assert.Nil(t, rewritten)
	assert.ErrorIs(t, err, ErrRelayImageDeliveryFailed)
	assert.NotContains(t, err.Error(), server.URL)
}

func TestRelayImageDownloadRetriesOnlyTransientFailures(t *testing.T) {
	t.Run("does not retry not found", func(t *testing.T) {
		var requests atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			requests.Add(1)
			http.NotFound(w, nil)
		}))
		defer server.Close()

		store := newRelayImageStore(t.TempDir(), "https://zmw.bon2.space", server.Client())
		_, err := store.downloadImageWithRetry(context.Background(), server.URL+"/missing.png")
		require.Error(t, err)
		assert.EqualValues(t, 1, requests.Load())
	})

	t.Run("retries a temporary server error", func(t *testing.T) {
		imageBytes := testRelayPNG(t)
		var requests atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if requests.Add(1) == 1 {
				http.Error(w, "temporary", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write(imageBytes)
		}))
		defer server.Close()

		store := newRelayImageStore(t.TempDir(), "https://zmw.bon2.space", server.Client())
		got, err := store.downloadImageWithRetry(context.Background(), server.URL+"/image.png")
		require.NoError(t, err)
		assert.Equal(t, imageBytes, got)
		assert.EqualValues(t, 2, requests.Load())
	})
}

func TestRelayImageDeliveryRewritesSSEImageEventsOnly(t *testing.T) {
	previousSecret := common.CryptoSecret
	common.CryptoSecret = "relay-image-sse-test-secret"
	t.Cleanup(func() { common.CryptoSecret = previousSecret })
	imageBytes := testRelayPNG(t)
	encoded := base64.StdEncoding.EncodeToString(imageBytes)
	store := newRelayImageStore(t.TempDir(), "https://zmw.bon2.space", nil)

	input := []byte("event: image_generation.completed\ndata: {\"type\":\"image_generation.completed\",\"b64_json\":\"" + encoded + "\"}\n\ndata: [DONE]\n\n")
	output, err := store.RewriteResponse(context.Background(), input, "text/event-stream")
	require.NoError(t, err)
	assert.Contains(t, string(output), "event: image_generation.completed")
	assert.Contains(t, string(output), "data: [DONE]")
	assert.Contains(t, string(output), "https://zmw.bon2.space/v1/images/relay/")
	assert.NotContains(t, string(output), "b64_json")
	assert.NotContains(t, string(output), "upstream")
}

func TestRelayImageErrorClassificationAndPreAcceptanceRetryScope(t *testing.T) {
	assert.True(t, IsRelayImageContentRejected(http.StatusBadRequest, []byte(`{"code":"content_policy_violation"}`)))
	assert.False(t, IsRelayImageContentRejected(http.StatusInternalServerError, []byte(`{"code":"content_policy_violation"}`)))
	assert.False(t, IsRelayImageContentRejected(http.StatusBadRequest, []byte(`{"message":"bad request"}`)))
	assert.False(t, IsRelayImageContentRejected(http.StatusBadRequest, []byte(`{"message":"内容不符合请求格式"}`)))
	assert.True(t, IsRelayImagePreAcceptanceConnectionError(&os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}))
	assert.False(t, IsRelayImagePreAcceptanceConnectionError(context.DeadlineExceeded))
	assert.False(t, IsRelayImagePreAcceptanceConnectionError(io.EOF))
}

func TestRelayImageUpstreamErrorMessagePreservesReasonAndRedactsSensitiveData(t *testing.T) {
	body := []byte(`{"error":{"message":"请求包含当前服务不支持的参数。查看 https://api.vendor.example/v1?access=secret Authorization: Bearer abc123 api_key:xyz sk-12345678901234567890"}}`)
	message := RelayImageUpstreamErrorMessage(http.StatusBadRequest, body, "https://api.vendor.example")

	assert.Contains(t, message, "请求包含当前服务不支持的参数")
	assert.Contains(t, message, "[链接已隐藏]")
	assert.Contains(t, message, "Bearer ***")
	assert.Contains(t, message, "api_key=***")
	assert.Contains(t, message, "sk-***")
	assert.NotContains(t, message, "vendor.example")
	assert.NotContains(t, message, "secret")
	assert.NotContains(t, message, "abc123")
	assert.NotContains(t, message, "xyz")
}

func TestRelayImageUpstreamErrorMessageSupportsPlainProviderMessageAndFallback(t *testing.T) {
	assert.Equal(t,
		"请求包含当前服务不支持的参数",
		RelayImageUpstreamErrorMessage(http.StatusBadRequest, []byte(`{"message":"请求包含当前服务不支持的参数"}`)),
	)
	assert.Equal(t, "图片生成失败（HTTP 502）", RelayImageUpstreamErrorMessage(http.StatusBadGateway, []byte(`<html>gateway error</html>`)))
}

func testRelayPNG(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 2, 2))
	img.Set(0, 0, color.RGBA{R: 255, A: 255})
	var output bytes.Buffer
	require.NoError(t, png.Encode(&output, img))
	return output.Bytes()
}
