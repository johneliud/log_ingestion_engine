package consumer

import (
	"context"
	"log/slog"
	"sync"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

// FallbackProcessor handles processing messages from fallback.
type FallbackProcessor struct {
	fallbackChannel <-chan model.EnrichedLog
	storageHandler  StorageHandler
}

// Returns a new fallback processor.
func NewFallbackProcessor(fallbackChannel <-chan model.EnrichedLog, storageHandler StorageHandler) *FallbackProcessor {
	return &FallbackProcessor{
		fallbackChannel: fallbackChannel,
		storageHandler:  storageHandler,
	}
}

// Start processes messages from fallback channel.
func (fallbackProcessor *FallbackProcessor) Start(applicationContext context.Context) <-chan struct{} {
	completed := make(chan struct{})
	var waitGroup sync.WaitGroup
	waitGroup.Go(func() {
		for {
			select {
			case <-applicationContext.Done():
				return
			case enrichedLog, ok := <-fallbackProcessor.fallbackChannel:
				if !ok {
					return
				}
				if err := fallbackProcessor.storageHandler.StoreBatch(enrichedLog.Destination, []model.EnrichedLog{enrichedLog}); err != nil {
					slog.Error("failed to store from fallback", "error", err, "destination", enrichedLog.Destination)
				}
			}
		}
	})
	go func() {
		waitGroup.Wait()
		close(completed)
	}()
	return completed
}
