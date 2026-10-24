package handler

import (
	"fmt"
	"net/http"
)

// Returns a handler responding to any request with a short plain-text acknowledgment.
func RootEndpointHandler() http.HandlerFunc {
	return func(responseWriter http.ResponseWriter, request *http.Request) {
		fmt.Fprintln(responseWriter, "log ingestion engine")
	}
}
