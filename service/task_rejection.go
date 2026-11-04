package service

import (
	"net/http"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/dto"
)

const TaskContentRejectedMessage = "内容不符合规范 请调整后再试"

var taskContentRejectionCodes = []string{
	"datainspectionfailed",
	"outputimagesensitivecontentdetected",
	"moderationblocked",
	"contentmoderationfailed",
	"contentpolicyviolation",
	"contentfilter",
	"policyviolation",
	"safetyviolation",
	"imagesafety",
	"prohibitedcontent",
	"sensitivecontent",
	"imageprohibitedcontent",
}

var taskContentRejectionPhrases = []string{
	"blocked by moderation",
	"blocked by safety",
	"content moderation",
	"content policy",
	"sensitive content",
	"policy violation",
	"safety filter",
	"safety violation",
	"safety policy",
	"prohibited content",
	"content violates",
	"violates policy",
	"内容违规",
	"内容不符合规范",
	"敏感内容",
	"违反内容政策",
}

func IsTaskContentRejection(reason string, code int, data any) bool {
	var text strings.Builder
	text.WriteString(reason)
	if code == 1026 {
		text.WriteString(" status_code_1026")
	}
	switch value := data.(type) {
	case string:
		text.WriteString(" ")
		text.WriteString(value)
	case []byte:
		text.WriteString(" ")
		text.Write(value)
	default:
		if value != nil {
			if encoded, err := common.Marshal(value); err == nil {
				text.WriteByte(' ')
				text.Write(encoded)
			}
		}
	}
	combined := strings.ToLower(text.String())
	for _, phrase := range taskContentRejectionPhrases {
		if strings.Contains(combined, phrase) {
			return true
		}
	}
	normalized := strings.Map(func(r rune) rune {
		if r >= 'A' && r <= 'Z' {
			return r + ('a' - 'A')
		}
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, combined)
	for _, code := range taskContentRejectionCodes {
		if strings.Contains(normalized, code) {
			return true
		}
	}
	return strings.Contains(normalized, "statuscode1026") || strings.Contains(normalized, "code1026")
}

func NormalizeTaskContentRejection(taskErr *dto.TaskError) bool {
	if taskErr == nil {
		return false
	}
	reason := taskErr.Message
	if taskErr.Error != nil {
		reason += " " + taskErr.Error.Error()
	}
	if !IsTaskContentRejection(reason, 0, nil) {
		return false
	}
	taskErr.Code = "content_rejected"
	taskErr.Message = TaskContentRejectedMessage
	taskErr.StatusCode = http.StatusBadRequest
	taskErr.LocalError = true
	taskErr.NoRetry = true
	taskErr.NoRefund = true
	return true
}
