/*
Copyright © 2022  Ron Lynn <dad@lynntribe.net>
*/
package cmd

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/docktermj/go-xyzzy-helpers/logger"
	"github.com/senzing/go-common/record"
	"github.com/senzing/senzing-tools/constant"
	"github.com/senzing/senzing-tools/envar"
	"github.com/senzing/senzing-tools/option"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile  string
	fileType string
	inputURL string
	logLevel string
)

const (
	defaultDelayInSeconds int    = 0
	defaultFileType       string = ""
	defaultInputURL       string = ""
	defaultOutputURL      string = ""
	defaultLogLevel       string = "error"
)

const (
	envVarReplacerCharNew string = "_"
	envVarReplacerCharOld string = "-"
)

// validate is 6203:  https://github.com/Senzing/knowledge-base/blob/main/lists/senzing-product-ids.md
const MessageIdFormat = "senzing-6203%04d"

var (
	buildIteration string = "0"
	buildVersion   string = "0.0.0"
	programName    string = fmt.Sprintf("move-%d", time.Now().Unix())
)

// ----------------------------------------------------------------------------
// roncewind's cheat sheet..  :-P
//go run . --inputURL "file:///home/roncewind/roncewind.git/move/bad_test.jsonl"
//go run . --inputURL "file:///home/roncewind/roncewind.git/move/loadtest-dataset-100.jsonl"
//go run . --inputURL "file:///home/roncewind/roncewind.git/move/loadtest-dataset-1M-with-datasource.jsonl"
//go run . --inputURL "https://public-read-access.s3.amazonaws.com/TestDataSets/SenzingTruthSet/truth-set-3.0.0.jsonl"
//go run . --inputURL "https://public-read-access.s3.amazonaws.com/TestDataSets/SenzingTruthSet/truth-set.json" --fileType jsonl

// ----------------------------------------------------------------------------
// rootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validates a JSON-lines file.",
	Long: `
	Welcome to validate!
	Validate the each line of a JSON-lines (JSONL) file conforms to the Generic Entity Specification.

	Usage example:

	validate --inputURL "file:///path/to/json/lines/file.jsonl"
	validate --inputURL "https://public-read-access.s3.amazonaws.com/TestDataSets/SenzingTruthSet/truth-set-3.0.0.jsonl"
	`,
	PreRun: func(cobraCommand *cobra.Command, args []string) {
		loadConfigurationFile(cobraCommand)
		loadOptions(cobraCommand)
		cobraCommand.SetVersionTemplate(constant.VersionTemplate)
	},
	Run: func(cmd *cobra.Command, args []string) {

		if !read() {
			cmd.Help()
		}

	},
}

// ----------------------------------------------------------------------------
// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := RootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

// // ----------------------------------------------------------------------------
// func init() {
// 	cobra.OnInitialize(initConfig)

// 	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.validate.yaml)")

// 	RootCmd.Flags().StringVarP(&fileType, "fileType", "", "", "file type override")
// 	viper.BindPFlag("fileType", RootCmd.Flags().Lookup("fileType"))
// 	RootCmd.Flags().StringVarP(&inputURL, "inputURL", "i", "", "input location")
// 	viper.BindPFlag("inputURL", RootCmd.Flags().Lookup("inputURL"))
// 	RootCmd.Flags().StringVarP(&logLevel, "logLevel", "", "", "set the logging level, default Error")
// 	viper.BindPFlag("logLevel", RootCmd.Flags().Lookup("logLevel"))
// }

// // ----------------------------------------------------------------------------
// // initConfig reads in config file and ENV variables if set.
// func initConfig() {
// 	if cfgFile != "" {
// 		// Use config file from the flag.
// 		viper.SetConfigFile(cfgFile)
// 	} else {
// 		// Find home directory.
// 		home, err := os.UserHomeDir()
// 		cobra.CheckErr(err)

// 		// Search config in <home directory>/.senzing-tools with name "config" (without extension).
// 		viper.AddConfigPath(home + "/.senzing-tools")
// 		viper.AddConfigPath(home)
// 		viper.AddConfigPath("/etc/senzing-tools")
// 		viper.SetConfigType("yaml")
// 		viper.SetConfigName("config")
// 	}

// 	if err := viper.ReadInConfig(); err != nil {
// 		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
// 			// Config file not found; ignore error
// 		} else {
// 			// Config file was found but another error was produced
// 			logger.LogMessageFromError(MessageIdFormat, 2001, "Config file found, but not loaded", err)
// 		}
// 	}

// 	viper.AutomaticEnv() // read in environment variables that match
// 	// all env vars should be prefixed with "SENZING_TOOLS_"
// 	viper.SetEnvPrefix("senzing_tools")
// 	viper.BindEnv("fileType")
// 	viper.BindEnv("inputURL")
// 	viper.BindEnv("logLevel")

// 	// setup local variables, in case they came from a config file
// 	//TODO:  why do I have to do this?  env vars and cmdline params get mapped
// 	//  automatically, this is only IF the var is in the config file
// 	fileType = viper.GetString("fileType")
// 	inputURL = viper.GetString("inputURL")
// 	logLevel = viper.GetString("logLevel")

// 	setLogLevel()
// }

// ----------------------------------------------------------------------------
func read() bool {
	//This assumes the URL includes a schema and path so, minimally:
	//  "s://p" where the schema is 's' and 'p' is the complete path
	if len(inputURL) < 5 {
		logger.LogMessage(MessageIdFormat, 2002, fmt.Sprintf("Check the inputURL parameter: %s", inputURL))
		return false
	}

	logger.LogMessage(MessageIdFormat, 2, fmt.Sprintf("Validating URL string: %s", inputURL))
	u, err := url.Parse(inputURL)
	if err != nil {
		logger.LogMessageFromError(MessageIdFormat, 9001, "Fatal error parsing inputURL.", err)
	}
	if u.Scheme == "file" {
		if strings.HasSuffix(u.Path, "jsonl") || strings.ToUpper(fileType) == "JSONL" {
			logger.LogMessage(MessageIdFormat, 3, "Validating as a JSONL file.")
			readJSONLFile(u.Path)
			return true
		} else {
			logger.LogMessage(MessageIdFormat, 2003, "If this is a valid JSONL file, please rename with the .jsonl extension or use the file type override (--fileType).")
		}
	} else if u.Scheme == "http" || u.Scheme == "https" {
		if strings.HasSuffix(u.Path, "jsonl") || strings.ToUpper(fileType) == "JSONL" {
			logger.LogMessage(MessageIdFormat, 4, "Validating as a JSONL resource.")
			readJSONLResource()
			return true
		} else {
			logger.LogMessage(MessageIdFormat, 2004, "If this is a valid JSONL file, please rename with the .jsonl extension or use the file type override (--fileType).")
		}
	} else {
		logger.LogMessage(MessageIdFormat, 9002, fmt.Sprintf("We don't handle %s input URLs.", u.Scheme))
	}
	return false
}

// ----------------------------------------------------------------------------
func readJSONLResource() {
	response, err := http.Get(inputURL)
	if err != nil {
		logger.LogMessageFromError(MessageIdFormat, 9003, "Fatal error retrieving inputURL.", err)
	}
	defer response.Body.Close()
	validateLines(response.Body)

}

// ----------------------------------------------------------------------------
func readJSONLFile(jsonFile string) {
	file, err := os.Open(jsonFile)
	if err != nil {
		logger.LogMessageFromError(MessageIdFormat, 9004, "Fatal error opening inputURL.", err)
	}
	defer file.Close()
	validateLines(file)

}

// ----------------------------------------------------------------------------
func validateLines(reader io.Reader) {
	scanner := bufio.NewScanner(reader)
	totalLines := 0
	noRecordId := 0
	noDataSource := 0
	malformed := 0
	badRecord := 0
	for scanner.Scan() {
		totalLines++
		str := strings.TrimSpace(scanner.Text())
		// ignore blank lines
		if len(str) > 0 {
			valid, err := record.Validate(str)
			if !valid {
				fmt.Println("Line", totalLines, err)
				if err != nil {
					if strings.Contains(err.Error(), "RECORD_ID") {
						noRecordId++
					} else if strings.Contains(err.Error(), "DATA_SOURCE") {
						noDataSource++
					} else if strings.Contains(err.Error(), "not well formed") {
						malformed++
					} else {
						badRecord++
					}
				}
			}
		}
	}
	if noRecordId > 0 {
		logger.LogMessage(MessageIdFormat, 5, fmt.Sprintf("%d line(s) had no RECORD_ID field.", noRecordId))
	}
	if noDataSource > 0 {
		logger.LogMessage(MessageIdFormat, 6, fmt.Sprintf("%d line(s) had no DATA_SOURCE field.", noDataSource))
	}
	if malformed > 0 {
		logger.LogMessage(MessageIdFormat, 7, fmt.Sprintf("%d line(s) are not well formed JSON-lines.", malformed))
	}
	if badRecord > 0 {
		logger.LogMessage(MessageIdFormat, 8, fmt.Sprintf("%d line(s) did not validate for an unknown reason.", badRecord))
	}
	logger.LogMessage(MessageIdFormat, 9, fmt.Sprintf("Validated %d lines, %d were bad.", totalLines, noRecordId+noDataSource+malformed+badRecord))
}

// ----------------------------------------------------------------------------
func init() {
	RootCmd.Flags().String(option.InputFileType, defaultFileType, option.InputFileTypeHelp)
	RootCmd.Flags().String(option.InputURL, defaultInputURL, option.InputURLHelp)
	RootCmd.Flags().String(option.LogLevel, defaultLogLevel, fmt.Sprintf(option.LogLevelHelp, envar.LogLevel))
}

// ----------------------------------------------------------------------------

// If a configuration file is present, load it.
func loadConfigurationFile(cobraCommand *cobra.Command) {
	configuration := ""
	configFlag := cobraCommand.Flags().Lookup(option.Configuration)
	if configFlag != nil {
		configuration = configFlag.Value.String()
	}
	if configuration != "" { // Use configuration file specified as a command line option.
		viper.SetConfigFile(configuration)
	} else { // Search for a configuration file.

		// Determine home directory.

		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Specify configuration file name.

		viper.SetConfigName("move")
		viper.SetConfigType("yaml")

		// Define search path order.

		viper.AddConfigPath(home + "/.senzing-tools")
		viper.AddConfigPath(home)
		viper.AddConfigPath("/etc/senzing-tools")
	}

	// If a config file is found, read it in.

	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Applying configuration file:", viper.ConfigFileUsed())
	}
}

// ----------------------------------------------------------------------------

// Configure Viper with user-specified options.
func loadOptions(cobraCommand *cobra.Command) {
	viper.AutomaticEnv()
	replacer := strings.NewReplacer(envVarReplacerCharOld, envVarReplacerCharNew)
	viper.SetEnvKeyReplacer(replacer)
	viper.SetEnvPrefix(constant.SetEnvPrefix)

	// Strings

	stringOptions := map[string]string{
		option.InputFileType: defaultFileType,
		option.InputURL:      defaultInputURL,
		option.LogLevel:      defaultLogLevel,
	}
	for optionKey, optionValue := range stringOptions {
		viper.SetDefault(optionKey, optionValue)
		viper.BindPFlag(optionKey, cobraCommand.Flags().Lookup(optionKey))
	}

}

// ----------------------------------------------------------------------------
func setLogLevel() {
	var level logger.Level = logger.LevelError
	if viper.IsSet("logLevel") {
		switch strings.ToUpper(logLevel) {
		case logger.LevelDebugName:
			level = logger.LevelDebug
		case logger.LevelErrorName:
			level = logger.LevelError
		case logger.LevelFatalName:
			level = logger.LevelFatal
		case logger.LevelInfoName:
			level = logger.LevelInfo
		case logger.LevelPanicName:
			level = logger.LevelPanic
		case logger.LevelTraceName:
			level = logger.LevelTrace
		case logger.LevelWarnName:
			level = logger.LevelWarn
		}
		logger.SetLevel(level)
	}
}
