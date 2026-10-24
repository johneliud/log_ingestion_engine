package ingest

import (
	"net/http"
	"testing"
)

func TestResolveClientIP_PrefersFirstEntryFromXForwardedFor(t *testing.T) {
	t.Parallel()
	request := &http.Request{
		Header:     make(http.Header),
		RemoteAddr: "192.168.1.10:12345",
	}
	request.Header.Set("X-Forwarded-For", "203.0.113.5, 198.51.100.2")
	if got := ResolveClientIP(request); got != "203.0.113.5" {
		t.Errorf("ResolveClientIP() = %q, want %q", got, "203.0.113.5")
	}
}

func TestResolveClientIP_TrimsWhitespaceFromXForwardedForEntry(t *testing.T) {
	t.Parallel()
	request := &http.Request{
		Header:     make(http.Header),
		RemoteAddr: "192.168.1.10:12345",
	}
	request.Header.Set("X-Forwarded-For", "  203.0.113.7  , 198.51.100.3")
	if got := ResolveClientIP(request); got != "203.0.113.7" {
		t.Errorf("ResolveClientIP() = %q, want %q", got, "203.0.113.7")
	}
}

func TestResolveClientIP_IgnoresEmptyFirstEntryInXForwardedForList(t *testing.T) {
	t.Parallel()
	request := &http.Request{
		Header:     make(http.Header),
		RemoteAddr: "192.168.1.10:12345",
	}
	request.Header.Set("X-Forwarded-For", " , 198.51.100.4")
	if got := ResolveClientIP(request); got != "192.168.1.10" {
		t.Errorf("ResolveClientIP() = %q, want %q", got, "192.168.1.10")
	}
}

func TestResolveClientIP_UsesSocketAddressHostWhenHeaderMissing(t *testing.T) {
	t.Parallel()
	request := &http.Request{
		Header:     make(http.Header),
		RemoteAddr: "203.0.113.9:54321",
	}
	if got := ResolveClientIP(request); got != "203.0.113.9" {
		t.Errorf("ResolveClientIP() = %q, want %q", got, "203.0.113.9")
	}
}

func TestResolveClientIP_UsesSocketAddressWhenNoPort(t *testing.T) {
	t.Parallel()
	request := &http.Request{
		Header:     make(http.Header),
		RemoteAddr: "203.0.113.11",
	}
	if got := ResolveClientIP(request); got != "203.0.113.11" {
		t.Errorf("ResolveClientIP() = %q, want %q", got, "203.0.113.11")
	}
}

func TestResolveClientIP_ReturnsUnknownWhenNothingKnown(t *testing.T) {
	t.Parallel()
	request := &http.Request{
		Header: make(http.Header),
	}
	if got := ResolveClientIP(request); got != "unknown" {
		t.Errorf("ResolveClientIP() = %q, want %q", got, "unknown")
	}

	if got := ResolveClientIP(nil); got != "unknown" {
		t.Errorf("ResolveClientIP(nil) = %q, want %q", got, "unknown")
	}
}
