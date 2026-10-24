package ingest

import (
	"net"
	"net/http"
	"strings"
)

// ResolveClientIP returns the source IP from X-Forwarded-For when present, otherwise the socket address host, otherwise unknown. X-Forwarded-For is client-controlled unless a trusted proxy overwrites it.
func ResolveClientIP(request *http.Request) string {
	if request == nil {
		return "unknown"
	}

	forwardedHeader := request.Header.Get("X-Forwarded-For")
	if forwardedHeader != "" {
		forwardedEntries := strings.Split(forwardedHeader, ",")
		if len(forwardedEntries) > 0 {
			firstEntry := strings.TrimSpace(forwardedEntries[0])
			if firstEntry != "" {
				return firstEntry
			}
		}
	}

	if request.RemoteAddr == "" {
		return "unknown"
	}

	host, _, splitError := net.SplitHostPort(request.RemoteAddr)
	if splitError == nil {
		if host == "" {
			return "unknown"
		}
		return host
	}

	if request.RemoteAddr == "" {
		return "unknown"
	}

	return request.RemoteAddr
}
