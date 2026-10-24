package router

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

func TestNewServiceRouter_UsesDefaultDestinationWhenUnmatched(t *testing.T) {
	t.Parallel()
	serviceRouter, routerError := NewServiceRouter("", string(model.DestinationService1))
	if routerError != nil {
		t.Fatalf("unexpected error: %v", routerError)
	}

	enrichedLog := model.EnrichedLog{LogEntry: model.LogEntry{Service: "unknownservice"}}
	routedLog, routeError := serviceRouter.Route(enrichedLog)
	if routeError != nil {
		t.Fatalf("unexpected route error: %v", routeError)
	}
	if routedLog.Destination != model.DestinationService1 {
		t.Errorf("Destination = %q, want %q", routedLog.Destination, model.DestinationService1)
	}
}

func TestNewServiceRouter_LoadsRulesFromConfigurationFile(t *testing.T) {
	t.Parallel()
	routingConfigurationPath := filepath.Join(t.TempDir(), "routing.json")
	routingConfigurationContents := `{"claims":"service2","billing":"service3"}`
	if writeError := os.WriteFile(routingConfigurationPath, []byte(routingConfigurationContents), 0600); writeError != nil {
		t.Fatalf("could not write routing configuration: %v", writeError)
	}

	serviceRouter, routerError := NewServiceRouter(routingConfigurationPath, string(model.DestinationService1))
	if routerError != nil {
		t.Fatalf("unexpected error: %v", routerError)
	}

	enrichedLog := model.EnrichedLog{LogEntry: model.LogEntry{Service: "claims"}}
	routedLog, routeError := serviceRouter.Route(enrichedLog)
	if routeError != nil {
		t.Fatalf("unexpected route error: %v", routeError)
	}
	if routedLog.Destination != model.DestinationService2 {
		t.Errorf("Destination = %q, want %q", routedLog.Destination, model.DestinationService2)
	}
}

func TestNewServiceRouter_UsesConfiguredDefaultDestination(t *testing.T) {
	t.Parallel()
	serviceRouter, routerError := NewServiceRouter("", string(model.DestinationService3))
	if routerError != nil {
		t.Fatalf("unexpected error: %v", routerError)
	}

	enrichedLog := model.EnrichedLog{LogEntry: model.LogEntry{Service: "other"}}
	routedLog, routeError := serviceRouter.Route(enrichedLog)
	if routeError != nil {
		t.Fatalf("unexpected route error: %v", routeError)
	}
	if routedLog.Destination != model.DestinationService3 {
		t.Errorf("Destination = %q, want %q", routedLog.Destination, model.DestinationService3)
	}
}

func TestNewServiceRouter_ReturnsErrorForInvalidConfiguration(t *testing.T) {
	t.Parallel()
	routingConfigurationPath := filepath.Join(t.TempDir(), "routing.json")
	if writeError := os.WriteFile(routingConfigurationPath, []byte(`{invalid`), 0600); writeError != nil {
		t.Fatalf("could not write routing configuration: %v", writeError)
	}

	_, routerError := NewServiceRouter(routingConfigurationPath, string(model.DestinationService1))
	if routerError == nil {
		t.Fatalf("expected an error for invalid routing configuration")
	}
}
