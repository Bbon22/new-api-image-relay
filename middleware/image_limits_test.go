package middleware

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateImageRequestChecksJSONImageDimensions(t *testing.T) {
	previousMode := gin.Mode()
	gin.SetMode(gin.TestMode)
	t.Cleanup(func() { gin.SetMode(previousMode) })

	tests := []struct {
		name       string
		width      uint32
		height     uint32
		wantStatus int
	}{
		{name: "valid dimensions", width: 1280, height: 720, wantStatus: http.StatusNoContent},
		{name: "zero width", width: 0, height: 720, wantStatus: http.StatusBadRequest},
		{name: "zero height", width: 1280, height: 0, wantStatus: http.StatusBadRequest},
		{name: "side exceeds limit", width: imageDimensionMax + 1, height: 1, wantStatus: http.StatusBadRequest},
		{name: "pixel count exceeds limit", width: imageDimensionMax, height: imageDimensionMax, wantStatus: http.StatusBadRequest},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, path := range []string{"/v1/images/generations", "/v1/edits"} {
				router := gin.New()
				router.POST(path, ValidateImageRequest(), func(c *gin.Context) {
					c.Status(http.StatusNoContent)
				})

				requestBody := fmt.Sprintf(
					`{"model":"test","prompt":"cat","image":"data:image/png;base64,%s"}`,
					base64.StdEncoding.EncodeToString(testPNGHeader(test.width, test.height)),
				)
				request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(requestBody))
				request.Header.Set("Content-Type", "application/json")
				recorder := httptest.NewRecorder()

				router.ServeHTTP(recorder, request)

				assert.Equal(t, test.wantStatus, recorder.Code, path)
				if test.wantStatus == http.StatusBadRequest {
					require.Contains(t, recorder.Body.String(), "invalid_image_input")
				}
			}
		})
	}
}

func testPNGHeader(width, height uint32) []byte {
	header := make([]byte, 24)
	copy(header[:8], []byte{137, 80, 78, 71, 13, 10, 26, 10})
	copy(header[12:16], "IHDR")
	binary.BigEndian.PutUint32(header[16:20], width)
	binary.BigEndian.PutUint32(header[20:24], height)
	return header
}
