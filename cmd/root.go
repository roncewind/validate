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

	"github.com/roncewind/szrecord"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string
	fileType string
	inputURL string
)

//go run . --inputURL "file:///home/roncewind/roncewind.git/move/bad_test.jsonl"
//go run . --inputURL "file:///home/roncewind/roncewind.git/move/loadtest-dataset-100.jsonl"
//go run . --inputURL "file:///home/roncewind/roncewind.git/move/loadtest-dataset-1M-with-datasource.jsonl"
//go run . --inputURL "https://public-read-access.s3.amazonaws.com/TestDataSets/SenzingTruthSet/truth-set-3.0.0.jsonl"
//go run . --inputURL "https://public-read-access.s3.amazonaws.com/TestDataSets/SenzingTruthSet/truth-set.json" --fileType jsonl

// rootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validates a file is in JSON-lines format and conforms to the generic entity specification.",
	Long: `To validate a JSON-lines file pass For example:

validate --inputURL "file:///path/to/json/lines/file.jsonl"
validate --inputURL "https://public-read-access.s3.amazonaws.com/TestDataSets/SenzingTruthSet/truth-set-3.0.0.jsonl"`,

Run: func(cmd *cobra.Command, args []string) {

	if( !read() ) {
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
func init() {
	cobra.OnInitialize(initConfig)

	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.validate.yaml)")

	RootCmd.Flags().StringVarP(&fileType, "fileType", "", "", "file type override")
	viper.BindPFlag("fileType", RootCmd.Flags().Lookup("fileType"))
	RootCmd.Flags().StringVarP(&inputURL, "inputURL", "i", "", "input location")
	viper.BindPFlag("inputURL", RootCmd.Flags().Lookup("inputURL"))
}

// ----------------------------------------------------------------------------
// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in <home directory>/.senzing with name "config" (without extension).
		viper.AddConfigPath(home+"/.senzing-tools")
		viper.AddConfigPath(home)
		viper.AddConfigPath("/etc/senzing-tools")
		viper.SetConfigType("yaml")
		viper.SetConfigName("config")
	}

	viper.AutomaticEnv() // read in environment variables that match
	// all env vars should be prefixed with "SENZING_TOOLS_"
	viper.SetEnvPrefix("senzing_tools")
	viper.BindEnv("fileType")
	viper.BindEnv("inputURL")

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}
}

// ----------------------------------------------------------------------------
func read() bool {
	//This assumes the URL includes a schema and path so, minimally:
	//  "s://p" where the schema is 's' and 'p' is the complete path
	if len(inputURL) < 5 {
		fmt.Printf("ERROR: check the inputURL parameter: %s\n", inputURL)
		return false
	}

	fmt.Println("Validating URL string: ",inputURL)
	u, err := url.Parse(inputURL)
	if err != nil {
		panic(err)
	}
	if u.Scheme == "file" {
		if strings.HasSuffix(u.Path, "jsonl") || strings.ToUpper(fileType) == "JSONL" {
			fmt.Println("Validating as a JSONL file.")
			readJSONLFile(u.Path)
			return true
		} else {
			fmt.Println("If this is a valid JSONL file, please rename with the .jsonl extension or use the file type override (--fileType).")
		}
	} else if u.Scheme == "http" || u.Scheme == "https" {
		if strings.HasSuffix(u.Path, "jsonl") || strings.ToUpper(fileType) == "JSONL" {
			fmt.Println("Validating as a JSONL resource.")
			readJSONLResource()
			return true
		} else {
			fmt.Println("If this is a valid JSONL file, please rename with the .jsonl extension or use the file type override (--fileType).")
		}
	} else {
		msg := fmt.Sprintf("We don't handle %s input URLs.", u.Scheme)
		panic(msg)
	}
	return false
}

// ----------------------------------------------------------------------------
func readJSONLResource(){
	response, err := http.Get(inputURL)
	if err != nil {
		panic(err)
	}
	defer response.Body.Close()
	validateLines(response.Body)

}

// ----------------------------------------------------------------------------
func readJSONLFile(jsonFile string){
	file, err := os.Open(jsonFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	validateLines(file)

}

// ----------------------------------------------------------------------------
func validateLines(reader io.Reader){

	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanLines)

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
			valid, err := szrecord.Validate(str)
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
		fmt.Printf("%d line(s) had no RECORD_ID field.\n", noRecordId)
	}
	if noDataSource > 0 {
		fmt.Printf("%d line(s) had no DATA_SOURCE field.\n", noDataSource)
	}
	if malformed > 0 {
		fmt.Printf("%d line(s) are not well formed JSON-lines.\n", malformed)
	}
	if badRecord > 0 {
		fmt.Printf("%d line(s) did not validate for an unknown reason.\n", badRecord)
	}
	fmt.Printf("Validated %d lines, %d were bad.\n", totalLines, noRecordId + noDataSource + malformed + badRecord)
}

