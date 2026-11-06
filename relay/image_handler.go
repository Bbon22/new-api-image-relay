package relay

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/logger"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	relayconstant "github.com/QuantumNous/new-api/relay/constant"
	"github.com/QuantumNous/new-api/relay/helper"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/setting/model_setting"

	"github.com/gin-gonic/gin"
)

func ImageHelper(c *gin.Context, info *relaycommon.RelayInfo) (newAPIError *types.NewAPIError) {
	info.InitChannelMeta(c)
	info.BillingImageCount = nil
	info.ImageRequestCount = 0

	imageReq, ok := info.Request.(*dto.ImageRequest)
	if !ok {
		return types.NewErrorWithStatusCode(fmt.Errorf("invalid request type, expected dto.ImageRequest, got %T", info.Request), types.ErrorCodeInvalidRequest, http.StatusBadRequest, types.ErrOptionWithSkipRetry())
	}

	request, err := common.DeepCopy(imageReq)
	if err != nil {
		return types.NewError(fmt.Errorf("failed to copy request to ImageRequest: %w", err), types.ErrorCodeInvalidRequest, types.ErrOptionWithSkipRetry())
	}

	err = helper.ModelMappedHelper(c, info, request)
	if err != nil {
		return types.NewError(err, types.ErrorCodeChannelModelMappedError, types.ErrOptionWithSkipRetry())
	}

	adaptor := GetAdaptor(info.ApiType)
	if adaptor == nil {
		return types.NewError(fmt.Errorf("invalid api type: %d", info.ApiType), types.ErrorCodeInvalidApiType, types.ErrOptionWithSkipRetry())
	}
	adaptor.Init(info)
	imageCount, err := request.ImageCount(false)
	if err != nil {
		return types.NewErrorWithStatusCode(err, types.ErrorCodeInvalidRequest, http.StatusBadRequest, types.ErrOptionWithSkipRetry())
	}

	var requestBody io.Reader
	var jsonData []byte

	hostImageEditReferences := info.ChannelMeta != nil && info.RelayMode == relayconstant.RelayModeImagesEdits && info.ChannelOtherSettings.HostImageEditReferences
	if !hostImageEditReferences && (model_setting.GetGlobalSettings().PassThroughRequestEnabled || info.ChannelSetting.PassThroughBodyEnabled) {
		storage, err := common.GetBodyStorage(c)
		if err != nil {
			return types.NewErrorWithStatusCode(err, types.ErrorCodeReadRequestBodyFailed, http.StatusBadRequest, types.ErrOptionWithSkipRetry())
		}
		if strings.Contains(c.Request.Header.Get("Content-Type"), "multipart/form-data") {
			requestBody = common.NewReplayableBodyReader(storage)
		} else {
			jsonData, err = storage.Bytes()
			if err != nil {
				return types.NewErrorWithStatusCode(err, types.ErrorCodeReadRequestBodyFailed, http.StatusBadRequest, types.ErrOptionWithSkipRetry())
			}
		}
	} else {
		convertedRequest, err := adaptor.ConvertImageRequest(c, info, *request)
		if err != nil {
			// An adaptor that already classified its rejection (status code
			// and retry policy) keeps that classification instead of being
			// downgraded to a retryable conversion failure.
			var apiErr *types.NewAPIError
			if errors.As(err, &apiErr) {
				return apiErr
			}
			return types.NewError(err, types.ErrorCodeConvertRequestFailed)
		}
		relaycommon.AppendRequestConversionFromRequest(info, convertedRequest)

		switch convertedRequest.(type) {
		case *bytes.Buffer:
			requestBody = convertedRequest.(io.Reader)
		default:
			jsonData, err = common.Marshal(convertedRequest)
			if err != nil {
				return types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
			}

			// apply param override
			if len(info.ParamOverride) > 0 {
				jsonData, err = relaycommon.ApplyParamOverrideWithRelayInfo(jsonData, info)
				if err != nil {
					return newAPIErrorFromParamOverride(err)
				}
			}

		}
	}
	if jsonData != nil {
		// This is a different trust boundary from ingress: channel overrides
		// and pass-through bodies can change the quantity actually submitted.
		var outbound struct {
			N *uint `json:"n"`
		}
		if err := common.Unmarshal(jsonData, &outbound); err != nil {
			return types.NewErrorWithStatusCode(fmt.Errorf("invalid image billing parameters: %w", err), types.ErrorCodeInvalidRequest, http.StatusBadRequest, types.ErrOptionWithSkipRetry())
		}
		quantityRequest := dto.ImageRequest{N: outbound.N}
		if quantityRequest.N == nil {
			quantityRequest.N = common.GetPointer(uint(imageCount))
		}
		imageCount, err = quantityRequest.ImageCount(false)
		if err != nil {
			return types.NewErrorWithStatusCode(err, types.ErrorCodeInvalidRequest, http.StatusBadRequest, types.ErrOptionWithSkipRetry())
		}
		logger.LogDebug(c, "image request body: %s", jsonData)
		body, closer, err := relaycommon.NewOutboundJSONBody(jsonData)
		if err != nil {
			return types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
		}
		defer closer.Close()
		requestBody = body
	}
	if billingErr := service.PrepareImageBillingForRequest(c, info, imageCount); billingErr != nil {
		return billingErr
	}

	resp, err := adaptor.DoRequest(c, info, requestBody)
	if err != nil {
		if service.IsRelayImagePreAcceptanceConnectionError(err) && info.RetryIndex < 2 {
			return types.NewErrorWithStatusCode(
				errors.New("当前生成服务繁忙，请稍后重试"),
				types.ErrorCodeImageConnectionFailed,
				http.StatusBadGateway,
			)
		}
		return imagePlatformError("当前生成服务繁忙，请稍后重试", types.ErrorCodeDoRequestFailed, http.StatusBadGateway, true)
	}
	var httpResp *http.Response
	if resp != nil {
		httpResp = resp.(*http.Response)
		info.IsStream = info.IsStream || strings.HasPrefix(httpResp.Header.Get("Content-Type"), "text/event-stream")
		if httpResp.StatusCode != http.StatusOK {
			if httpResp.StatusCode == http.StatusCreated && info.ApiType == constant.APITypeReplicate {
				// replicate channel returns 201 Created when using Prefer: wait, treat it as success.
				httpResp.StatusCode = http.StatusOK
			} else {
				body, _ := io.ReadAll(io.LimitReader(httpResp.Body, 1<<20))
				_ = httpResp.Body.Close()
				if service.IsRelayImageContentRejected(httpResp.StatusCode, body) {
					return imagePlatformError(service.TaskContentRejectedMessage, types.ErrorCodePromptBlocked, http.StatusBadRequest, true, types.ErrOptionWithNoRefund())
				}
				message := service.RelayImageUpstreamErrorMessage(httpResp.StatusCode, body, common.UpstreamAddresses(c)...)
				return imagePlatformError(message, types.ErrorCodeDoRequestFailed, http.StatusBadGateway, true)
			}
		}
	}
	if httpResp != nil && httpResp.Body != nil {
		slotWaitStarted := time.Now()
		release, acquired := service.AcquireRelayImageRewrite(c.Request.Context())
		slotWait := time.Since(slotWaitStarted)
		if !acquired {
			common.SysLog(common.LogText("relay image rewrite slot wait canceled: elapsed=%s", slotWait.Round(time.Millisecond)))
			return imagePlatformError("媒体交付失败，请稍后重试", types.ErrorCodeBadResponseBody, http.StatusBadGateway, true)
		}
		defer release()
		if slotWait >= 250*time.Millisecond {
			common.SysLog(common.LogText("relay image rewrite slot wait slow: elapsed=%s", slotWait.Round(time.Millisecond)))
		}
		originalBody := httpResp.Body
		readStarted := time.Now()
		body, readErr := io.ReadAll(io.LimitReader(originalBody, (128<<20)+1))
		_ = originalBody.Close()
		if readErr != nil || len(body) > 128<<20 {
			common.SysLog(common.LogText("relay image response read failed: bytes=%d elapsed=%s", len(body), time.Since(readStarted).Round(time.Millisecond)))
			return imagePlatformError("媒体交付失败，请稍后重试", types.ErrorCodeBadResponseBody, http.StatusBadGateway, true)
		}
		if elapsed := time.Since(readStarted); elapsed >= time.Second {
			common.SysLog(common.LogText("relay image response read slow: bytes=%d elapsed=%s", len(body), elapsed.Round(time.Millisecond)))
		}
		rewriteStarted := time.Now()
		rewritten, rewriteErr := service.RewriteRelayImageResponse(c.Request.Context(), body, httpResp.Header.Get("Content-Type"))
		if rewriteErr != nil {
			failureKind := "delivery"
			if errors.Is(rewriteErr, service.ErrRelayImageRejected) {
				failureKind = "rejected"
				common.SysLog(common.LogText("relay image response rewrite failed: kind=%s bytes=%d elapsed=%s", failureKind, len(body), time.Since(rewriteStarted).Round(time.Millisecond)))
				return imagePlatformError(service.TaskContentRejectedMessage, types.ErrorCodePromptBlocked, http.StatusBadRequest, true, types.ErrOptionWithNoRefund())
			}
			if errors.Is(rewriteErr, service.ErrRelayImageGeneration) {
				failureKind = "generation"
				common.SysLog(common.LogText("relay image response rewrite failed: kind=%s bytes=%d elapsed=%s", failureKind, len(body), time.Since(rewriteStarted).Round(time.Millisecond)))
				return imagePlatformError("当前生成服务繁忙，请稍后重试", types.ErrorCodeDoRequestFailed, http.StatusBadGateway, true)
			}
			common.SysLog(common.LogText("relay image response rewrite failed: kind=%s bytes=%d elapsed=%s", failureKind, len(body), time.Since(rewriteStarted).Round(time.Millisecond)))
			return imagePlatformError("媒体交付失败，请稍后重试", types.ErrorCodeBadResponseBody, http.StatusBadGateway, true)
		}
		if elapsed := time.Since(rewriteStarted); elapsed >= 3*time.Second {
			common.SysLog(common.LogText("relay image response rewrite slow: bytes=%d elapsed=%s", len(body), elapsed.Round(time.Millisecond)))
		}
		httpResp.Body = io.NopCloser(bytes.NewReader(rewritten))
		httpResp.ContentLength = int64(len(rewritten))
		httpResp.Header.Set("Content-Length", fmt.Sprintf("%d", len(rewritten)))
	}

	usage, newAPIError := adaptor.DoResponse(c, httpResp, info)
	if newAPIError != nil {
		return imagePlatformError("当前生成服务繁忙，请稍后重试", types.ErrorCodeBadResponse, http.StatusBadGateway, true)
	}

	// The log content shows the settled count: the count the handler derived
	// from the upstream response when it did, otherwise the reserved quantity.
	imageN := info.RequestedImageCount()
	if info.BillingImageCount != nil {
		imageN = *info.BillingImageCount
	} else if count, ok := info.PriceData.OtherRatios()["n"]; ok && info.PriceData.UsePrice && count >= 1 && count <= dto.MaxImageN {
		imageN = common.QuotaRound(count)
	}

	if usage.(*dto.Usage).TotalTokens == 0 {
		usage.(*dto.Usage).TotalTokens = 1
	}
	if usage.(*dto.Usage).PromptTokens == 0 {
		usage.(*dto.Usage).PromptTokens = 1
	}

	quality := request.Quality
	if quality == "" {
		quality = "standard"
	}

	var logContent []*common.Message

	if len(request.Size) > 0 {
		logContent = append(logContent, common.NewMessage("Size {{size}}", map[string]any{"size": request.Size}))
	}
	if len(quality) > 0 {
		logContent = append(logContent, common.NewMessage("Quality {{quality}}", map[string]any{"quality": quality}))
	}
	if imageN > 0 {
		logContent = append(logContent, common.NewMessage("Image count {{count}}", map[string]any{"count": imageN}))
	}

	service.PostTextConsumeQuota(c, info, usage.(*dto.Usage), logContent)
	return nil
}

func imagePlatformError(message string, code types.ErrorCode, status int, skipRetry bool, options ...types.NewAPIErrorOptions) *types.NewAPIError {
	if skipRetry {
		options = append(options, types.ErrOptionWithSkipRetry())
	}
	return types.NewErrorWithStatusCode(errors.New(message), code, status, options...)
}
