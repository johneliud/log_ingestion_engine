package config

import (
	"errors"
	"fmt"
	"maps"
	"os"

	"github.com/joho/godotenv"
)

// Reads a KEY-VALUE configuration file with godotenv, returning no settings and no error when the file does not exist.
func readOptionalConfigurationFile(configurationFilePath string) (map[string]string, error) {
	if _, pathError := os.Stat(configurationFilePath); pathError != nil {
		if errors.Is(pathError, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("cannot access configuration file %s: %w", configurationFilePath, pathError)
	}
	configurationFileSettings, parsingError := godotenv.Read(configurationFilePath)
	if parsingError != nil {
		return nil, fmt.Errorf("configuration file %s is invalid: %w", configurationFilePath, parsingError)
	}
	return configurationFileSettings, nil
}

// Builds the final settings map from the real environment plus the configuration file, with the real environment winning conflicts.
func mergedEnvironmentWithFileSettings(environmentVariables map[string]string, configurationFileSettings map[string]string) map[string]string {
	mergedSettings := make(map[string]string, len(environmentVariables)+len(configurationFileSettings))
	maps.Copy(mergedSettings, configurationFileSettings)
	maps.Copy(mergedSettings, environmentVariables)
	return mergedSettings
}
