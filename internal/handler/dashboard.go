package handler

import (
	"io/fs"
	"net/http"
	"strings"

	"github.com/johneliud/log_ingestion_engine/internal/dashboard"
)

// Serves the embedded dashboard page at /dashboard, assets under /dashboard/css and /dashboard/js, and root-level files such as the architecture diagram under /dashboard/.
func NewDashboardHandler() http.HandlerFunc {
	staticAssets, subError := fs.Sub(dashboard.Assets, "static")
	if subError != nil {
		panic("embedded dashboard static directory is missing: " + subError.Error())
	}
	staticAssetsFileServer := http.FileServerFS(staticAssets)
	rootAssetsFileServer := http.FileServerFS(dashboard.Assets)
	return func(responseWriter http.ResponseWriter, request *http.Request) {
		request.URL.Path = strings.TrimPrefix(request.URL.Path, "/dashboard")
		assetName := strings.TrimPrefix(request.URL.Path, "/")
		if assetName == "" {
			rootAssetsFileServer.ServeHTTP(responseWriter, request)
			return
		}
		assetEntry, statError := fs.Stat(staticAssets, assetName)
		if statError == nil && !assetEntry.IsDir() {
			staticAssetsFileServer.ServeHTTP(responseWriter, request)
			return
		}
		rootAssetsFileServer.ServeHTTP(responseWriter, request)
	}
}
