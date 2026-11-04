package controller

import (
	"net/http"

	"github.com/QuantumNous/new-api/service"
	"github.com/gin-gonic/gin"
)

func RelayImageContent(c *gin.Context) {
	if err := service.ServeRelayImageContent(c, c.Param("image_id"), c.Param("artifact_key")); err != nil {
		c.Header("Cache-Control", "private, no-store")
		c.JSON(http.StatusNotFound, gin.H{
			"error": gin.H{
				"message": "Image not found or expired",
				"type":    "image_not_found",
				"code":    "image_not_found",
			},
		})
	}
}
