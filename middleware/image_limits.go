package middleware

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/gin-gonic/gin"
)

const (
	imageRequestBodyLimit = int64(24 << 20)
	imageReferenceLimit   = int64(4 << 20)
	imageReferencesMax    = 9
	imageReferencesTotal  = int64(20 << 20)
	imageDimensionMax     = 8192
	imagePixelsMax        = int64(67_000_000)
	imageHeaderReadLimit  = 64 << 10
)

var imageUploadFieldPattern = regexp.MustCompile(`^(image|images|reference_image|reference_images)(\[\d*\])?$`)

// ValidateImageRequest enforces the relay's image input contract before
// distribution or billing. It intentionally inspects only multipart file
// headers; the image body is not decoded into a full bitmap.
func ValidateImageRequest() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Body != nil {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, imageRequestBodyLimit)
		}
		if c.Request.ContentLength > imageRequestBodyLimit {
			c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{"error": gin.H{
				"message": "image request body exceeds 24 MB",
				"type":    "invalid_request_error",
				"code":    "image_request_too_large",
			}})
			return
		}
		if !isImageRelayRequestPath(c.Request.URL.Path) ||
			(!strings.HasPrefix(strings.ToLower(c.GetHeader("Content-Type")), "multipart/form-data") &&
				!strings.HasPrefix(strings.ToLower(c.GetHeader("Content-Type")), "application/json")) {
			c.Next()
			return
		}
		if strings.HasPrefix(strings.ToLower(c.GetHeader("Content-Type")), "application/json") {
			if err := validateJSONImageInputs(c); err != nil {
				if common.IsRequestBodyTooLargeError(err) {
					abortImageInput(c, http.StatusRequestEntityTooLarge, "image request body exceeds 24 MB")
					return
				}
				abortImageInput(c, http.StatusBadRequest, "reference image must be a valid JPG, PNG, or WebP file within the size and dimension limits")
				return
			}
			c.Next()
			return
		}

		form, err := common.ParseMultipartFormReusable(c)
		if err != nil {
			if common.IsRequestBodyTooLargeError(err) {
				abortImageInput(c, http.StatusRequestEntityTooLarge, "image request body exceeds 24 MB")
				return
			}
			abortImageInput(c, http.StatusBadRequest, "invalid multipart image request")
			return
		}
		count := 0
		var total int64
		for field, headers := range form.File {
			if !imageUploadFieldPattern.MatchString(field) && field != "input_image" && field != "mask" {
				continue
			}
			for _, header := range headers {
				count++
				if count > imageReferencesMax {
					abortImageInput(c, http.StatusBadRequest, "at most 9 reference images are allowed")
					return
				}
				if header.Size < 0 || header.Size > imageReferenceLimit {
					abortImageInput(c, http.StatusRequestEntityTooLarge, "each reference image must not exceed 4 MB")
					return
				}
				total += header.Size
				if total > imageReferencesTotal {
					abortImageInput(c, http.StatusRequestEntityTooLarge, "reference images must not exceed 20 MB in total")
					return
				}
				file, openErr := header.Open()
				if openErr != nil {
					abortImageInput(c, http.StatusBadRequest, "reference image could not be read")
					return
				}
				kind, width, height, headerErr := inspectImageHeader(file)
				_ = file.Close()
				if headerErr != nil || kind == "" {
					abortImageInput(c, http.StatusBadRequest, "reference image must be a valid JPG, PNG, or WebP file")
					return
				}
				if !imageDimensionsAllowed(width, height) {
					abortImageInput(c, http.StatusBadRequest, "reference image dimensions exceed the allowed limit")
					return
				}
			}
		}
		c.Next()
	}
}

func validateJSONImageInputs(c *gin.Context) error {
	storage, err := common.GetBodyStorage(c)
	if err != nil {
		return err
	}
	raw, err := storage.Bytes()
	if err != nil {
		return err
	}
	var payload any
	if err := common.Unmarshal(raw, &payload); err != nil {
		return err
	}
	count := 0
	var total int64
	var inspect func(any) error
	inspect = func(value any) error {
		switch typed := value.(type) {
		case map[string]any:
			for _, nested := range typed {
				if err := inspect(nested); err != nil {
					return err
				}
			}
		case []any:
			for _, nested := range typed {
				if err := inspect(nested); err != nil {
					return err
				}
			}
		case string:
			if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(typed)), "data:image/") {
				return nil
			}
			comma := strings.IndexByte(typed, ',')
			if comma < 0 || !strings.Contains(strings.ToLower(typed[:comma]), ";base64") {
				return errors.New("image data URL must be base64 encoded")
			}
			encoded := typed[comma+1:]
			if len(encoded) > base64.StdEncoding.EncodedLen(int(imageReferenceLimit)) {
				return errors.New("image exceeds the per-file size limit")
			}
			imageBytes, decodeErr := base64.StdEncoding.DecodeString(encoded)
			if decodeErr != nil {
				imageBytes, decodeErr = base64.RawStdEncoding.DecodeString(encoded)
			}
			if decodeErr != nil || len(imageBytes) == 0 || int64(len(imageBytes)) > imageReferenceLimit {
				return errors.New("invalid image data")
			}
			count++
			total += int64(len(imageBytes))
			if count > imageReferencesMax || total > imageReferencesTotal {
				return errors.New("image references exceed the request limits")
			}
			kind, width, height, headerErr := inspectImageHeader(bytes.NewReader(imageBytes))
			if headerErr != nil || kind == "" || !imageDimensionsAllowed(width, height) {
				return errors.New("invalid image header or dimensions")
			}
		}
		return nil
	}
	return inspect(payload)
}

func abortImageInput(c *gin.Context, status int, message string) {
	c.AbortWithStatusJSON(status, gin.H{"error": gin.H{
		"message": message,
		"type":    "invalid_request_error",
		"code":    "invalid_image_input",
	}})
}

func imageDimensionsAllowed(width, height int) bool {
	return width > 0 && height > 0 && width <= imageDimensionMax && height <= imageDimensionMax && int64(width)*int64(height) <= imagePixelsMax
}

func isImageRelayRequestPath(path string) bool {
	return strings.HasPrefix(path, "/v1/images/") || path == "/v1/edits"
}

func inspectImageHeader(reader io.Reader) (string, int, int, error) {
	data, err := io.ReadAll(io.LimitReader(reader, imageHeaderReadLimit))
	if err != nil {
		return "", 0, 0, err
	}
	if len(data) >= 24 && bytes.Equal(data[:8], []byte{137, 80, 78, 71, 13, 10, 26, 10}) && string(data[12:16]) == "IHDR" {
		width, height := binary.BigEndian.Uint32(data[16:20]), binary.BigEndian.Uint32(data[20:24])
		return "png", int(width), int(height), nil
	}
	if len(data) >= 12 && bytes.Equal(data[:4], []byte("RIFF")) && bytes.Equal(data[8:12], []byte("WEBP")) {
		return inspectWebPHeader(data)
	}
	if len(data) >= 2 && data[0] == 0xff && data[1] == 0xd8 {
		width, height, err := inspectJPEGHeader(data)
		return "jpeg", width, height, err
	}
	return "", 0, 0, errors.New("unsupported image header")
}

func inspectJPEGHeader(data []byte) (int, int, error) {
	for index := 2; index+9 < len(data); {
		if data[index] != 0xff {
			index++
			continue
		}
		for index < len(data) && data[index] == 0xff {
			index++
		}
		if index >= len(data) {
			break
		}
		marker := data[index]
		index++
		if marker == 0xd8 || marker == 0xd9 {
			continue
		}
		if marker >= 0xd0 && marker <= 0xd7 || marker == 0x01 {
			continue
		}
		if index+2 > len(data) {
			break
		}
		segmentLength := int(binary.BigEndian.Uint16(data[index : index+2]))
		if segmentLength < 2 || index+segmentLength > len(data) {
			break
		}
		if isJPEGStartOfFrame(marker) && segmentLength >= 7 {
			height := int(binary.BigEndian.Uint16(data[index+3 : index+5]))
			width := int(binary.BigEndian.Uint16(data[index+5 : index+7]))
			return width, height, nil
		}
		index += segmentLength
	}
	return 0, 0, fmt.Errorf("JPEG dimensions not found in header")
}

func isJPEGStartOfFrame(marker byte) bool {
	return marker >= 0xc0 && marker <= 0xc3 || marker >= 0xc5 && marker <= 0xc7 || marker >= 0xc9 && marker <= 0xcb || marker >= 0xcd && marker <= 0xcf
}

func inspectWebPHeader(data []byte) (string, int, int, error) {
	if len(data) < 30 {
		return "", 0, 0, errors.New("WebP header is incomplete")
	}
	chunk := string(data[12:16])
	switch chunk {
	case "VP8X":
		width := 1 + (int(data[24]) | int(data[25])<<8 | int(data[26])<<16)
		height := 1 + (int(data[27]) | int(data[28])<<8 | int(data[29])<<16)
		return "webp", width, height, nil
	case "VP8 ":
		if len(data) < 30 || data[23] != 0x9d || data[24] != 0x01 || data[25] != 0x2a {
			return "", 0, 0, errors.New("invalid WebP VP8 header")
		}
		return "webp", int(binary.LittleEndian.Uint16(data[26:28]) & 0x3fff), int(binary.LittleEndian.Uint16(data[28:30]) & 0x3fff), nil
	case "VP8L":
		if data[20] != 0x2f {
			return "", 0, 0, errors.New("invalid WebP VP8L header")
		}
		width := 1 + (int(data[21]) | int(data[22]&0x3f)<<8)
		height := 1 + (int(data[22]>>6) | int(data[23])<<2 | int(data[24]&0x0f)<<10)
		return "webp", width, height, nil
	default:
		return "", 0, 0, fmt.Errorf("unsupported WebP chunk %q", chunk)
	}
}
