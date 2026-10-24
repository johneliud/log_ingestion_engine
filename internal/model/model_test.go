package model

import "testing"

func TestLogLevelConstants_MatchClientContract(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name           string
		logLevel       LogLevel
		expectedString string
	}{
		{"info level matches the uppercase client contract", LogLevelInfo, "INFO"},
		{"warn level matches the uppercase client contract", LogLevelWarn, "WARN"},
		{"error level matches the uppercase client contract", LogLevelError, "ERROR"},
		{"debug level matches the uppercase client contract", LogLevelDebug, "DEBUG"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if string(testCase.logLevel) != testCase.expectedString {
				t.Errorf("%s = %q, want %q", testCase.name, testCase.logLevel, testCase.expectedString)
			}
		})
	}
}

func TestDestinationConstants_MatchStorageContract(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name           string
		destination    Destination
		expectedString string
	}{
		{"service1 destination matches the storage database name", DestinationService1, "service1"},
		{"service2 destination matches the storage database name", DestinationService2, "service2"},
		{"service3 destination matches the storage database name", DestinationService3, "service3"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if string(testCase.destination) != testCase.expectedString {
				t.Errorf("%s = %q, want %q", testCase.name, testCase.destination, testCase.expectedString)
			}
		})
	}
}
