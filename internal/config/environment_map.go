package config

import "strings"

// Converts the KEY-VALUE pairs returned by os.Environ into a lookup map.
func mapFromOSEnviron(keyValuePairs []string) map[string]string {
	environmentVariables := make(map[string]string, len(keyValuePairs))

	for _, keyValuePair := range keyValuePairs {
		settingName, settingValue, separatorFound := strings.Cut(keyValuePair, "=")
		if !separatorFound {
			continue
		}
		environmentVariables[settingName] = settingValue
	}
	return environmentVariables
}
