package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/handler"
	"github.com/johneliud/log_ingestion_engine/internal/metrics"
)

const shutdownGracePeriod = 5 * time.Second

// Loads the application configuration, serves the placeholder endpoint and exits only after a clean shutdown.
func main() {
	applicationContext, applicationContextCancellation := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer applicationContextCancellation()

	applicationConfiguration, configurationError := config.NewApplicationConfiguration()
	if configurationError != nil {
		slog.Error("loading the application configuration failed", "configuration_error", configurationError)
		os.Exit(1)
	}
	logConfigurationSummary(applicationConfiguration)

	if serverError := runHTTPServerUntilContextIsCancelled(applicationContext, applicationConfiguration); serverError != nil {
		slog.Error("http server stopped unexpectedly", "server_error", serverError)
		os.Exit(1)
	}
	slog.Info("shutdown complete", "application_environment", applicationConfiguration.ApplicationEnvironment)
}

// Lists the effective startup settings without exposing any RabbitMQ credential.
func logConfigurationSummary(applicationConfiguration config.ApplicationConfiguration) {
	slog.Info("application configuration loaded",
		"http_port", applicationConfiguration.HTTPPort,
		"application_environment", applicationConfiguration.ApplicationEnvironment,
	)
}

// Builds the request router with the ingestion, metrics page, metrics snapshot, dashboard and home page routes wired.
func buildRequestRouter(applicationConfiguration config.ApplicationConfiguration) *http.ServeMux {
	requestRouter := http.NewServeMux()
	requestRouter.HandleFunc("POST /logs", handler.NewLogIngestionHandler(applicationConfiguration.MaximumBodySizeInBytes))
	metricsRegistry := metrics.NewMetrics(applicationConfiguration.MetricsThroughputWindow, nil)
	requestRouter.HandleFunc("GET /metrics", handler.MetricsPageEndpointHandler())
	requestRouter.HandleFunc("GET /metrics.json", handler.NewMetricsHandler(metricsRegistry, applicationConfiguration))
	dashboardHandler := handler.NewDashboardHandler()
	requestRouter.HandleFunc("GET /dashboard", dashboardHandler)
	requestRouter.HandleFunc("GET /dashboard/", dashboardHandler)
	requestRouter.HandleFunc("GET /{$}", handler.RootEndpointHandler())
	return requestRouter
}

// Serves the root endpoint and POST /logs handler until the context is cancelled, then shuts the server down within the shutdown grace period.
func runHTTPServerUntilContextIsCancelled(applicationContext context.Context, applicationConfiguration config.ApplicationConfiguration) error {
	requestRouter := buildRequestRouter(applicationConfiguration)

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", applicationConfiguration.HTTPPort),
		Handler: requestRouter,
	}

	serverListenErrorChannel := make(chan error, 1)
	go func() {
		serverListenErrorChannel <- httpServer.ListenAndServe()
	}()
	select {
	case serverListenError := <-serverListenErrorChannel:
		if !errors.Is(serverListenError, http.ErrServerClosed) {
			return serverListenError
		}
	case <-applicationContext.Done():
	}

	shutdownContext, shutdownContextCancellation := context.WithTimeout(context.Background(), shutdownGracePeriod)
	defer shutdownContextCancellation()
	return httpServer.Shutdown(shutdownContext)
}
