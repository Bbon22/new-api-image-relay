package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"sort"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/service"
	"github.com/gin-gonic/gin"
)

const (
	hostedEditReferenceMaxCount = 9
	hostedEditReferenceMaxBytes = int64(4 << 20)
	hostedEditRequestMaxBytes   = int64(20 << 20)
)

type hostedEditReference struct {
	field string
	data  []byte
}

type hostedEditReferenceIO struct {
	fetch func(context.Context, string) ([]byte, error)
	store func(context.Context, []byte) (string, error)
}

func convertHostedImageEditRequest(c *gin.Context, request dto.ImageRequest) (any, error) {
	return convertHostedImageEditRequestWithIO(c, request, hostedEditReferenceIO{
		fetch: service.FetchRelayImageReference,
		store: service.StoreRelayImageReference,
	})
}

func convertHostedImageEditRequestWithIO(c *gin.Context, request dto.ImageRequest, referenceIO hostedEditReferenceIO) (any, error) {
	if c == nil || c.Request == nil {
		return nil, errors.New("invalid image edit request")
	}
	if referenceIO.fetch == nil || referenceIO.store == nil {
		return nil, errors.New("image edit reference storage is unavailable")
	}
	storage, err := common.GetBodyStorage(c)
	if err != nil {
		return nil, errors.New("invalid image edit request")
	}
	raw, err := storage.Bytes()
	if err != nil {
		return nil, errors.New("invalid image edit request")
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("model", request.Model); err != nil {
		return nil, err
	}
	references := make([]hostedEditReference, 0, 2)
	contentType := strings.ToLower(c.GetHeader("Content-Type"))
	if strings.HasPrefix(contentType, "multipart/form-data") {
		err = collectHostedMultipartEdit(c, raw, writer, &references, referenceIO)
	} else if strings.HasPrefix(contentType, "application/json") {
		err = collectHostedJSONEdit(c, raw, writer, &references, referenceIO)
	} else {
		err = errors.New("image edit request must use multipart or JSON")
	}
	if err != nil {
		_ = writer.Close()
		return nil, err
	}
	if len(references) == 0 || len(references) > hostedEditReferenceMaxCount {
		_ = writer.Close()
		return nil, errors.New("image edit requires 1 to 9 reference images")
	}

	var total int64
	for _, reference := range references {
		total += int64(len(reference.data))
		if total > hostedEditRequestMaxBytes {
			_ = writer.Close()
			return nil, errors.New("reference images exceed the 20 MB total limit")
		}
	}
	for _, reference := range references {
		imageURL, err := referenceIO.store(c.Request.Context(), reference.data)
		if err != nil {
			_ = writer.Close()
			return nil, errors.New("reference image could not be hosted")
		}
		if err := writer.WriteField(reference.field, imageURL); err != nil {
			_ = writer.Close()
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	c.Request.Header.Set("Content-Type", writer.FormDataContentType())
	return &body, nil
}

func collectHostedMultipartEdit(c *gin.Context, raw []byte, writer *multipart.Writer, references *[]hostedEditReference, referenceIO hostedEditReferenceIO) error {
	mediaType, params, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || mediaType != "multipart/form-data" || params["boundary"] == "" {
		return errors.New("invalid multipart image edit request")
	}
	reader := multipart.NewReader(bytes.NewReader(raw), params["boundary"])
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return errors.New("invalid multipart image edit request")
		}
		name := part.FormName()
		field, isReference := hostedEditReferenceField(name)
		if isReference {
			if len(*references) >= hostedEditReferenceMaxCount {
				_ = part.Close()
				return errors.New("at most 9 reference images are allowed")
			}
			var imageBytes []byte
			if part.FileName() != "" {
				imageBytes, err = io.ReadAll(io.LimitReader(part, hostedEditReferenceMaxBytes+1))
				if err == nil && int64(len(imageBytes)) > hostedEditReferenceMaxBytes {
					err = errors.New("reference image exceeds the size limit")
				}
			} else {
				var value []byte
				value, err = io.ReadAll(io.LimitReader(part, hostedEditReferenceMaxBytes*2+4096))
				if err == nil {
					imageBytes, err = referenceIO.fetch(c.Request.Context(), string(value))
				}
			}
			if err == nil {
				err = service.ValidateRelayImageReferenceBytes(imageBytes)
			}
			_ = part.Close()
			if err != nil {
				return errors.New("reference image is invalid")
			}
			*references = append(*references, hostedEditReference{field: field, data: imageBytes})
			continue
		}
		if part.FileName() != "" {
			_ = part.Close()
			continue
		}
		value, readErr := io.ReadAll(io.LimitReader(part, hostedEditRequestMaxBytes+1))
		_ = part.Close()
		if readErr != nil || int64(len(value)) > hostedEditRequestMaxBytes {
			return errors.New("image edit field exceeds the request size limit")
		}
		if name == "model" {
			continue
		}
		if name == "" {
			continue
		}
		if err := writer.WriteField(name, string(value)); err != nil {
			return err
		}
	}
}

func collectHostedJSONEdit(c *gin.Context, raw []byte, writer *multipart.Writer, references *[]hostedEditReference, referenceIO hostedEditReferenceIO) error {
	var fields map[string]json.RawMessage
	if err := common.Unmarshal(raw, &fields); err != nil || fields == nil {
		return errors.New("invalid JSON image edit request")
	}
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value := fields[name]
		if name == "model" {
			continue
		}
		if name == "reference_image_urls" {
			var urls []string
			if err := common.Unmarshal(value, &urls); err != nil || len(urls) == 0 {
				return errors.New("reference_image_urls must contain image URLs")
			}
			if len(*references)+len(urls) > hostedEditReferenceMaxCount {
				return errors.New("at most 9 reference images are allowed")
			}
			for _, imageURL := range urls {
				imageBytes, err := referenceIO.fetch(c.Request.Context(), imageURL)
				if err != nil {
					return errors.New("reference image is invalid")
				}
				*references = append(*references, hostedEditReference{field: "image", data: imageBytes})
			}
			continue
		}
		if field, isReference := hostedEditReferenceField(name); isReference {
			values, err := hostedEditJSONStrings(value)
			if err != nil {
				return errors.New("reference image is invalid")
			}
			if len(*references)+len(values) > hostedEditReferenceMaxCount {
				return errors.New("at most 9 reference images are allowed")
			}
			for _, input := range values {
				imageBytes, err := referenceIO.fetch(c.Request.Context(), input)
				if err != nil {
					return errors.New("reference image is invalid")
				}
				*references = append(*references, hostedEditReference{field: field, data: imageBytes})
			}
			continue
		}
		var stringValue string
		if err := json.Unmarshal(value, &stringValue); err == nil {
			value = []byte(stringValue)
		} else {
			value = bytes.TrimSpace(value)
		}
		if err := writer.WriteField(name, string(value)); err != nil {
			return err
		}
	}
	return nil
}

func hostedEditReferenceField(name string) (string, bool) {
	switch {
	case name == "image", name == "image[]", name == "images", name == "images[]",
		name == "reference_image", name == "reference_images", name == "reference_images[]",
		strings.HasPrefix(name, "image["):
		return "image", true
	case name == "mask":
		return "mask", true
	default:
		return "", false
	}
}

func hostedEditJSONStrings(raw json.RawMessage) ([]string, error) {
	var single string
	if err := json.Unmarshal(raw, &single); err == nil {
		return []string{single}, nil
	}
	var values []string
	if err := json.Unmarshal(raw, &values); err != nil || len(values) == 0 {
		return nil, errors.New("expected an image URL or URL list")
	}
	return values, nil
}
