package openai

import (
	"bytes"
	"context"
	"image"
	"image/color"
	"image/png"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestHostedImageEditConvertsMultipartURLTextToHostedURLText(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var input bytes.Buffer
	writer := multipart.NewWriter(&input)
	require.NoError(t, writer.WriteField("model", "gpt-image-2"))
	require.NoError(t, writer.WriteField("prompt", "edit the image"))
	require.NoError(t, writer.WriteField("image", "https://customer.example/reference.png"))
	require.NoError(t, writer.Close())

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/edits", &input)
	c.Request.Header.Set("Content-Type", writer.FormDataContentType())
	imageBytes := hostedEditTestPNG(t)
	var fetched, stored int
	converted, err := convertHostedImageEditRequestWithIO(c, dto.ImageRequest{Model: "gpt-image-2"}, hostedEditReferenceIO{
		fetch: func(_ context.Context, input string) ([]byte, error) {
			fetched++
			require.Equal(t, "https://customer.example/reference.png", input)
			return imageBytes, nil
		},
		store: func(_ context.Context, got []byte) (string, error) {
			stored++
			require.Equal(t, imageBytes, got)
			return "https://relay.example/v1/images/relay/ref_a/reference/content?access=signed", nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, fetched)
	require.Equal(t, 1, stored)

	body, ok := converted.(*bytes.Buffer)
	require.True(t, ok)
	replayed := httptest.NewRequest(http.MethodPost, "/v1/images/edits", bytes.NewReader(body.Bytes()))
	replayed.Header.Set("Content-Type", c.Request.Header.Get("Content-Type"))
	require.NoError(t, replayed.ParseMultipartForm(1<<20))
	require.Equal(t, "gpt-image-2", replayed.FormValue("model"))
	require.Equal(t, "edit the image", replayed.FormValue("prompt"))
	require.Equal(t, []string{"https://relay.example/v1/images/relay/ref_a/reference/content?access=signed"}, replayed.MultipartForm.Value["image"])
	require.Empty(t, replayed.MultipartForm.File["image"])
}

func TestHostedImageEditConvertsAsyncReferenceImageURLsJSONToRepeatedImageFields(t *testing.T) {
	gin.SetMode(gin.TestMode)
	input := []byte(`{"model":"gpt-image-2","prompt":"edit the image","reference_image_urls":["https://customer.example/a.png","https://customer.example/b.png"],"n":1}`)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/edits", bytes.NewReader(input))
	c.Request.Header.Set("Content-Type", "application/json")
	imageBytes := hostedEditTestPNG(t)
	stored := 0
	converted, err := convertHostedImageEditRequestWithIO(c, dto.ImageRequest{Model: "gpt-image-2"}, hostedEditReferenceIO{
		fetch: func(_ context.Context, input string) ([]byte, error) {
			return imageBytes, nil
		},
		store: func(_ context.Context, got []byte) (string, error) {
			stored++
			require.Equal(t, imageBytes, got)
			return "https://relay.example/reference/" + string(rune('0'+stored)), nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, 2, stored)

	body, ok := converted.(*bytes.Buffer)
	require.True(t, ok)
	replayed := httptest.NewRequest(http.MethodPost, "/v1/images/edits", bytes.NewReader(body.Bytes()))
	replayed.Header.Set("Content-Type", c.Request.Header.Get("Content-Type"))
	require.NoError(t, replayed.ParseMultipartForm(1<<20))
	require.Equal(t, "gpt-image-2", replayed.FormValue("model"))
	require.Equal(t, "edit the image", replayed.FormValue("prompt"))
	require.Equal(t, "1", replayed.FormValue("n"))
	require.Equal(t, []string{"https://relay.example/reference/1", "https://relay.example/reference/2"}, replayed.MultipartForm.Value["image"])
	require.Empty(t, replayed.MultipartForm.Value["reference_image_urls"])
	require.Empty(t, replayed.MultipartForm.File["image"])
}

func TestHostedImageEditAcceptsFileUploadThenSendsURLText(t *testing.T) {
	gin.SetMode(gin.TestMode)
	imageBytes := hostedEditTestPNG(t)
	var input bytes.Buffer
	writer := multipart.NewWriter(&input)
	require.NoError(t, writer.WriteField("model", "gpt-image-2"))
	require.NoError(t, writer.WriteField("prompt", "edit the image"))
	part, err := writer.CreateFormFile("image", "reference.png")
	require.NoError(t, err)
	_, err = part.Write(imageBytes)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/edits", &input)
	c.Request.Header.Set("Content-Type", writer.FormDataContentType())
	converted, err := convertHostedImageEditRequestWithIO(c, dto.ImageRequest{Model: "gpt-image-2"}, hostedEditReferenceIO{
		fetch: func(context.Context, string) ([]byte, error) {
			t.Fatal("uploaded image should not be fetched from a URL")
			return nil, nil
		},
		store: func(_ context.Context, got []byte) (string, error) {
			require.Equal(t, imageBytes, got)
			return "https://relay.example/reference/uploaded", nil
		},
	})
	require.NoError(t, err)

	body, ok := converted.(*bytes.Buffer)
	require.True(t, ok)
	replayed := httptest.NewRequest(http.MethodPost, "/v1/images/edits", bytes.NewReader(body.Bytes()))
	replayed.Header.Set("Content-Type", c.Request.Header.Get("Content-Type"))
	require.NoError(t, replayed.ParseMultipartForm(1<<20))
	require.Equal(t, []string{"https://relay.example/reference/uploaded"}, replayed.MultipartForm.Value["image"])
	require.Empty(t, replayed.MultipartForm.File["image"])
}

func hostedEditTestPNG(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 2, 2))
	img.Set(0, 0, color.RGBA{R: 255, A: 255})
	var output bytes.Buffer
	require.NoError(t, png.Encode(&output, img))
	return output.Bytes()
}
