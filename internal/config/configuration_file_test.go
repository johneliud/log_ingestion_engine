package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Creates the file at configurationFilePath with configurationFileContents so a test can exercise the loader.
func writeConfigurationFile(t *testing.T, configurationFilePath string, configurationFileContents string) {
	t.Helper()
	if writeFileError := os.WriteFile(configurationFilePath, []byte(configurationFileContents), 0600); writeFileError != nil {
		t.Fatalf("could not write the test configuration file: %v", writeFileError)
	}
}

func TestReadOptionalConfigurationFile_ParsesGodotenvFileContents(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name                              string
		configurationFileContents         string
		expectedConfigurationFileSettings map[string]string
	}{
		{
			name:                              "plain assignment",
			configurationFileContents:         "RABBITMQ_HOST=localhost\n",
			expectedConfigurationFileSettings: map[string]string{"RABBITMQ_HOST": "localhost"},
		},
		{
			name:                              "payload assignment without spacing around the equals sign",
			configurationFileContents:         "HTTP_PORT=9443",
			expectedConfigurationFileSettings: map[string]string{"HTTP_PORT": "9443"},
		},
		{
			name:                              "assignment with extra spaces and indentation",
			configurationFileContents:         "  RETRY_BACKOFFS = 1s, 5s  \n",
			expectedConfigurationFileSettings: map[string]string{"RETRY_BACKOFFS": "1s, 5s"},
		},
		{
			name:                              "double quoted value",
			configurationFileContents:         "RABBITMQ_PASS=\"secret value\"\n",
			expectedConfigurationFileSettings: map[string]string{"RABBITMQ_PASS": "secret value"},
		},
		{
			name:                              "single quoted value keeps the literal text",
			configurationFileContents:         "RABBITMQ_PASS='$not_expanded'\n",
			expectedConfigurationFileSettings: map[string]string{"RABBITMQ_PASS": "$not_expanded"},
		},
		{
			name:                              "value containing an equals sign",
			configurationFileContents:         "RABBITMQ_PASS=a=b\n",
			expectedConfigurationFileSettings: map[string]string{"RABBITMQ_PASS": "a=b"},
		},
		{
			name:                              "export prefixed assignment",
			configurationFileContents:         "export HTTP_PORT=9443\n",
			expectedConfigurationFileSettings: map[string]string{"HTTP_PORT": "9443"},
		},
		{
			name:                              "blank lines and comments are skipped",
			configurationFileContents:         "\n# a comment\nHTTP_PORT=9443\n\n  # an indented comment\n",
			expectedConfigurationFileSettings: map[string]string{"HTTP_PORT": "9443"},
		},
		{
			name:                              "duplicate keys keep the last value",
			configurationFileContents:         "HTTP_PORT=9000\nHTTP_PORT=9443\n",
			expectedConfigurationFileSettings: map[string]string{"HTTP_PORT": "9443"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			configurationFilePath := filepath.Join(t.TempDir(), "loader.env")
			writeConfigurationFile(t, configurationFilePath, testCase.configurationFileContents)
			configurationFileSettings, fileError := readOptionalConfigurationFile(configurationFilePath)
			if fileError != nil {
				t.Fatalf("expected the contents to parse, got error: %v", fileError)
			}

			if len(configurationFileSettings) != len(testCase.expectedConfigurationFileSettings) {
				t.Fatalf("parsed %d settings, want %d", len(configurationFileSettings), len(testCase.expectedConfigurationFileSettings))
			}

			for expectedSettingName, expectedSettingValue := range testCase.expectedConfigurationFileSettings {
				if configurationFileSettings[expectedSettingName] != expectedSettingValue {
					t.Errorf("setting %q = %q, want %q", expectedSettingName, configurationFileSettings[expectedSettingName], expectedSettingValue)
				}
			}
		})
	}
}

func TestReadOptionalConfigurationFile_RejectsMalformedLines(t *testing.T) {
	t.Parallel()
	configurationFilePath := filepath.Join(t.TempDir(), "broken.env")
	writeConfigurationFile(t, configurationFilePath, "HTTP_PORT\n")
	_, fileError := readOptionalConfigurationFile(configurationFilePath)
	if fileError == nil {
		t.Fatalf("expected an error but parsing succeeded")
	}

	if !strings.Contains(fileError.Error(), "configuration file") {
		t.Errorf("error %q does not name the configuration file", fileError.Error())
	}
}

func TestReadOptionalConfigurationFile_HandlesMissingAndPresentFiles(t *testing.T) {
	t.Parallel()
	t.Run("missing file yields no settings and no error", func(t *testing.T) {
		t.Parallel()
		configurationFileSettings, fileError := readOptionalConfigurationFile(filepath.Join(t.TempDir(), "absent.env"))
		if fileError != nil {
			t.Fatalf("expected no error, got: %v", fileError)
		}

		if configurationFileSettings != nil {
			t.Errorf("expected no settings, got %v", configurationFileSettings)
		}
	})
	t.Run("present file yields parsed settings", func(t *testing.T) {
		t.Parallel()
		configurationFilePath := filepath.Join(t.TempDir(), "flat.env")
		writeConfigurationFile(t, configurationFilePath, "APP_ENV=staging\n")
		configurationFileSettings, fileError := readOptionalConfigurationFile(configurationFilePath)
		if fileError != nil {
			t.Fatalf("expected no error, got: %v", fileError)
		}

		if configurationFileSettings["APP_ENV"] != "staging" {
			t.Errorf("APP_ENV = %q, want %q", configurationFileSettings["APP_ENV"], "staging")
		}
	})
}

func TestMergedEnvironmentWithFileSettings_KeepsProcessEnvironmentPrecedence(t *testing.T) {
	t.Parallel()
	environmentVariables := map[string]string{"HTTP_PORT": "9443"}
	configurationFileSettings := map[string]string{"HTTP_PORT": "9000", "DB_DIR": "./dbdata"}
	mergedSettings := mergedEnvironmentWithFileSettings(environmentVariables, configurationFileSettings)
	if mergedSettings["HTTP_PORT"] != "9443" {
		t.Errorf("HTTP_PORT = %q, want the overriding environment value \"9443\"", mergedSettings["HTTP_PORT"])
	}

	if mergedSettings["DB_DIR"] != "./dbdata" {
		t.Errorf("DB_DIR = %q, want the file value %q", mergedSettings["DB_DIR"], "./dbdata")
	}
}

func TestNewApplicationConfiguration_LoadsConfigurationFileAutomatically(t *testing.T) {
	for _, conflictingSettingName := range []string{"HTTP_PORT", "RABBITMQ_HOST", "RABBITMQ_PORT", "RABBITMQ_USER", "PIPELINE_WORKER_COUNT"} {
		if _, settingIsKnown := os.LookupEnv(conflictingSettingName); settingIsKnown {
			t.Skipf("the process environment defines %s and would override the configuration file", conflictingSettingName)
		}
	}

	configurationFilePath := filepath.Join(t.TempDir(), "auto.env")
	configurationFileContents := strings.Join([]string{
		"HTTP_PORT=9443",
		"RABBITMQ_HOST=broker.internal",
		"RABBITMQ_PORT=5673",
		"RABBITMQ_USER=svc_writer",
		"RABBITMQ_PASS=automated_secret",
		"# a comment",
		"PIPELINE_WORKER_COUNT=8",
	}, "\n")

	writeConfigurationFile(t, configurationFilePath, configurationFileContents)
	t.Setenv(environmentNameConfigurationFilePath, configurationFilePath)
	applicationConfiguration, configurationError := NewApplicationConfiguration()
	if configurationError != nil {
		t.Fatalf("expected the configuration file to load, got error: %v", configurationError)
	}

	if applicationConfiguration.HTTPPort != 9443 {
		t.Errorf("HTTPPort = %d, want 9443", applicationConfiguration.HTTPPort)
	}

	if applicationConfiguration.RabbitMQHost != "broker.internal" {
		t.Errorf("RabbitMQHost = %q, want %q", applicationConfiguration.RabbitMQHost, "broker.internal")
	}

	if applicationConfiguration.RabbitMQPort != 5673 {
		t.Errorf("RabbitMQPort = %d, want 5673", applicationConfiguration.RabbitMQPort)
	}

	if applicationConfiguration.RabbitMQUsername != "svc_writer" {
		t.Errorf("RabbitMQUsername = %q, want %q", applicationConfiguration.RabbitMQUsername, "svc_writer")
	}

	if applicationConfiguration.PipelineWorkerCount != 8 {
		t.Errorf("PipelineWorkerCount = %d, want 8", applicationConfiguration.PipelineWorkerCount)
	}
}
