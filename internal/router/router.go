package router

import (
	"fmt"
	"log/slog"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

// Loads routing rules from configuration.
type ServiceRouter struct {
	rules              map[string]model.Destination
	defaultDestination model.Destination
}

// Returns a new service router.
func NewServiceRouter(rules map[string]string, defaultDestination string) (*ServiceRouter, error) {
	routingRules := make(map[string]model.Destination)
	defaultDestinationValue := model.Destination(defaultDestination)
	if defaultDestinationValue == "" {
		defaultDestinationValue = model.DestinationService1
	}
	if !isValidDestination(defaultDestinationValue) {
		return nil, fmt.Errorf("unknown default destination %q", defaultDestination)
	}

	for serviceName, destinationName := range rules {
		dest := model.Destination(destinationName)
		if !isValidDestination(dest) {
			return nil, fmt.Errorf("unknown destination %q for service %q", destinationName, serviceName)
		}
		routingRules[serviceName] = dest
	}

	return &ServiceRouter{rules: routingRules, defaultDestination: defaultDestinationValue}, nil
}

func isValidDestination(destination model.Destination) bool {
	switch destination {
	case model.DestinationService1, model.DestinationService2, model.DestinationService3:
		return true
	default:
		return false
	}
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
