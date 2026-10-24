package router

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

// Loads routing rules from configuration.
type ServiceRouter struct {
	rules              map[string]model.Destination
	defaultDestination model.Destination
}

// Returns a new service router.
func NewServiceRouter(routingConfigurationFilePath string, defaultDestination string) (*ServiceRouter, error) {
	routingRules := make(map[string]model.Destination)
	defaultDestinationValue := model.Destination(defaultDestination)
	if defaultDestinationValue == "" {
		defaultDestinationValue = model.DestinationService1
	}

	if routingConfigurationFilePath == "" {
		return &ServiceRouter{rules: routingRules, defaultDestination: defaultDestinationValue}, nil
	}

	fileContents, fileError := os.ReadFile(routingConfigurationFilePath)
	if fileError != nil {
		if errors.Is(fileError, os.ErrNotExist) {
			return &ServiceRouter{rules: routingRules, defaultDestination: defaultDestinationValue}, nil
		}
		return nil, fmt.Errorf("reading routing configuration file: %w", fileError)
	}

	var routingConfiguration map[string]string
	if unmarshalError := json.Unmarshal(fileContents, &routingConfiguration); unmarshalError != nil {
		return nil, fmt.Errorf("parsing routing configuration file: %w", unmarshalError)
	}

	for serviceName, destinationName := range routingConfiguration {
		routingRules[serviceName] = model.Destination(destinationName)
	}

	return &ServiceRouter{rules: routingRules, defaultDestination: defaultDestinationValue}, nil
}

// Routes an enriched log to its destination.
func (serviceRouter *ServiceRouter) Route(enrichedLog model.EnrichedLog) (model.EnrichedLog, error) {
	destination, exists := serviceRouter.rules[enrichedLog.Service]
	if !exists {
		slog.Warn("service has no routing rule, using default destination", "service", enrichedLog.Service, "default_destination", string(serviceRouter.defaultDestination))
		enrichedLog.Destination = serviceRouter.defaultDestination
		return enrichedLog, nil
	}
	enrichedLog.Destination = destination
	return enrichedLog, nil
}
