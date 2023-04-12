/*
Copyright © 2022  Ron Lynn <dad@lynntribe.net>
*/
package cmd

import (
	"bufio"
	"compress/gzip"
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

const (
	defaultFileType string = ""
	defaultInputURL string = ""
	defaultLogLevel string = "error"
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
	programName    string = fmt.Sprintf("validate-%d", time.Now().Unix())
)

// ----------------------------------------------------------------------------
// rootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validates a JSON-lines file.",
	Long: `
	Welcome to validate!
	Validate the each line of a JSON-lines (JSONL) file conforms to the Generic Entity Specification.

	Usage example:

	validate --input-url "file:///path/to/json/lines/file.jsonl"
	validate --input-url "https://public-read-access.s3.amazonaws.com/TestDataSets/SenzingTruthSet/truth-set-3.0.0.jsonl"
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

// ----------------------------------------------------------------------------
func read() bool {

	inputURL := viper.GetString(option.InputURL)
	inputURLLen := len(inputURL)

	if inputURLLen == 0 {
		//assume stdin
		return readStdin()
	}

	//This assumes the URL includes a schema and path so, minimally:
	//  "s://p" where the schema is 's' and 'p' is the complete path
	if len(inputURL) < 5 {
		logger.LogMessage(MessageIdFormat, 2002, fmt.Sprintf("Check the inputURL parameter: %s", inputURL))
		return false
	}

	fileType := viper.GetString(option.InputFileType)
	logger.LogMessage(MessageIdFormat, 2, fmt.Sprintf("Validating URL string: %s", inputURL))
	fmt.Println("inputURL:", inputURL)
	u, err := url.Parse(inputURL)
	if err != nil {
		logger.LogMessageFromError(MessageIdFormat, 9001, "Fatal error parsing inputURL.", err)
		return false
	}
	if u.Scheme == "file" {
		if strings.HasSuffix(u.Path, "jsonl") || strings.ToUpper(fileType) == "JSONL" {
			logger.LogMessage(MessageIdFormat, 3, "Validating as a JSONL file.")
			return readJSONLFile(u.Path)
		} else if strings.HasSuffix(u.Path, "gz") || strings.ToUpper(fileType) == "GZ" {
			logger.LogMessage(MessageIdFormat, 4, "Validating a GZ file.")
			return readGZFile(u.Path)
		} else {
			logger.LogMessage(MessageIdFormat, 2003, "If this is a valid JSONL file, please rename with the .jsonl extension or use the file type override (--fileType).")
		}
	} else if u.Scheme == "http" || u.Scheme == "https" {
		fmt.Println("scheme:", u.Scheme)
		if strings.HasSuffix(u.Path, "jsonl") || strings.ToUpper(fileType) == "JSONL" {
			logger.LogMessage(MessageIdFormat, 5, "Validating as a JSONL resource.")
			fmt.Println("validate jsonl")
			return readJSONLResource(inputURL)
		} else if strings.HasSuffix(u.Path, "gz") || strings.ToUpper(fileType) == "GZ" {
			fmt.Println("validate gz")
			logger.LogMessage(MessageIdFormat, 6, "Validating a GZ resource.")
			return readGZResource(inputURL)
		} else {
			fmt.Println("ugh")
			logger.LogMessage(MessageIdFormat, 2004, "If this is a valid JSONL file, please rename with the .jsonl extension or use the file type override (--fileType).")
		}
	} else {
		logger.LogMessage(MessageIdFormat, 9002, fmt.Sprintf("We don't handle %s input URLs.", u.Scheme))
	}
	return false
}

// ----------------------------------------------------------------------------
func readJSONLResource(jsonURL string) bool {
	response, err := http.Get(jsonURL)

	if err != nil {
		fmt.Println("unable to get:", jsonURL)
		logger.LogMessageFromError(MessageIdFormat, 9003, "Fatal error retrieving inputURL.", err)
		return false
	}
	defer response.Body.Close()
	validateLines(response.Body)
	return true
}

// ----------------------------------------------------------------------------
func readJSONLFile(jsonFile string) bool {
	file, err := os.Open(jsonFile)
	if err != nil {
		logger.LogMessageFromError(MessageIdFormat, 9004, "Fatal error opening inputURL.", err)
		return false
	}
	defer file.Close()
	validateLines(file)
	return true
}

// ----------------------------------------------------------------------------
func readStdin() bool {
	info, err := os.Stdin.Stat()
	if err != nil {
		logger.LogMessageFromError(MessageIdFormat, 9005, "Fatal error opening stdin.", err)
		return false
	}
	//printFileInfo(info)

	if info.Mode()&os.ModeNamedPipe == os.ModeNamedPipe {

		reader := bufio.NewReader(os.Stdin)
		validateLines(reader)
		return true
	}
	logger.LogMessageFromError(MessageIdFormat, 9006, "Fatal error stdin not piped.", err)
	return false
}

// ----------------------------------------------------------------------------
func readGZResource(gzURL string) bool {
	response, err := http.Get(gzURL)
	if err != nil {
		logger.LogMessageFromError(MessageIdFormat, 9009, "Fatal error retrieving inputURL.", err)
		return false
	}
	defer response.Body.Close()
	reader, err := gzip.NewReader(response.Body)
	if err != nil {
		logger.LogMessageFromError(MessageIdFormat, 9010, "Fatal error reading inputURL.", err)
		return false
	}
	defer reader.Close()
	validateLines(reader)
	return true
}

// ----------------------------------------------------------------------------

// opens and reads a JSONL file that has been Gzipped
func readGZFile(gzFile string) bool {
	gzipfile, err := os.Open(gzFile)
	if err != nil {
		logger.LogMessageFromError(MessageIdFormat, 9007, "Fatal error opening inputURL.", err)
		return false
	}
	defer gzipfile.Close()

	reader, err := gzip.NewReader(gzipfile)
	if err != nil {
		logger.LogMessageFromError(MessageIdFormat, 9008, "Fatal error reading inputURL.", err)
		return false
	}
	defer reader.Close()
	validateLines(reader)
	return true
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
	fmt.Printf("Validated %d lines, %d were bad.\n", totalLines, noRecordId+noDataSource+malformed+badRecord)
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

		viper.SetConfigName("validate")
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
		switch strings.ToUpper(viper.GetString(option.LogLevel)) {
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

// ----------------------------------------------------------------------------
func printFileInfo(info os.FileInfo) {
	fmt.Println("name: ", info.Name())
	fmt.Println("size: ", info.Size())
	fmt.Println("mode: ", info.Mode())
	fmt.Println("mod time: ", info.ModTime())
	fmt.Println("is dir: ", info.IsDir())
	if info.Mode()&os.ModeDevice == os.ModeDevice {
		fmt.Println("detected device: ", os.ModeDevice)
	}
	if info.Mode()&os.ModeCharDevice == os.ModeCharDevice {
		fmt.Println("detected char device: ", os.ModeCharDevice)
	}
	if info.Mode()&os.ModeNamedPipe == os.ModeNamedPipe {
		fmt.Println("detected named pipe: ", os.ModeNamedPipe)
	}
	fmt.Printf("\n\n")
}
