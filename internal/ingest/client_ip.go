package ingest

import (
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// Returns the source IP from X-Forwarded-For when a trusted proxy forwarded the request and the first forwarded entry is a valid IP, otherwise the socket address host, otherwise unknown. X-Forwarded-For is client-controlled unless a trusted proxy overwrites it.
func ResolveClientIP(request *http.Request, trustedProxies []string) string {
	if request == nil {
		return "unknown"
	}

	sourceIPAddress := chooseSocketAddressHost(request)

	if forwardedHeader := request.Header.Get("X-Forwarded-For"); forwardedHeader != "" && hasForwardedFromTrustedProxy(request, trustedProxies) {
		firstEntry := ""
		for entry := range strings.SplitSeq(forwardedHeader, ",") {
			firstEntry = strings.TrimSpace(entry)
			break
		}
		if parsedAddress, parseError := netip.ParseAddr(firstEntry); parseError == nil {
			sourceIPAddress = parsedAddress.String()
		}
	}

	return sourceIPAddress
}

// Returns the host portion of the socket address, or unknown when the address is absent.
func chooseSocketAddressHost(request *http.Request) string {
	if request.RemoteAddr == "" {
		return "unknown"
	}

	host, _, splitError := net.SplitHostPort(request.RemoteAddr)
	if splitError != nil {
		return request.RemoteAddr
	}
	if host == "" {
		return "unknown"
	}

	return host
}

// Returns whether a trusted proxy forwarded the request.
func hasForwardedFromTrustedProxy(request *http.Request, trustedProxies []string) bool {
	if len(trustedProxies) == 0 {
		return false
	}

	peerAddress := request.RemoteAddr
	if host, _, splitError := net.SplitHostPort(peerAddress); splitError == nil {
		peerAddress = host
	}

	peerAddressValue, peerAddressParseError := netip.ParseAddr(peerAddress)
	if peerAddressParseError != nil {
		return false
	}

	for _, trustedProxy := range trustedProxies {
		prefix, prefixParseError := netip.ParsePrefix(trustedProxy)
		if prefixParseError != nil {
			continue
		}
		if prefix.Contains(peerAddressValue) {
			return true
		}
	}

	return false
}
