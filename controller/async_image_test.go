package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveAsyncImageRelayPathUsesEditRelayForReferenceImageURLs(t *testing.T) {
	path, err := resolveAsyncImageRelayPath(
		"/v1/images/generations",
		"application/json",
		[]byte(`{"model":"gpt-image-2","prompt":"edit","reference_image_urls":["https://cdn.example/input.png"]}`),
	)
	require.NoError(t, err)
	require.Equal(t, "/v1/images/edits", path)
}

func TestResolveAsyncImageRelayPathKeepsTextToImageAsGeneration(t *testing.T) {
	path, err := resolveAsyncImageRelayPath(
		"/v1/images/generations",
		"application/json",
		[]byte(`{"model":"gpt-image-2","prompt":"a cat"}`),
	)
	require.NoError(t, err)
	require.Equal(t, "/v1/images/generations", path)
}

func TestResolveAsyncImageRelayPathRejectsEmptyReferenceList(t *testing.T) {
	_, err := resolveAsyncImageRelayPath(
		"/v1/images/generations",
		"application/json",
		[]byte(`{"model":"gpt-image-2","prompt":"edit","reference_image_urls":[]}`),
	)
	require.ErrorContains(t, err, "reference_image_urls")
}
