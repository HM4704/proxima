/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package main

import (
	"fmt"
	"os"

	"github.com/lunfardo314/proxima/proxi/console"
	"github.com/lunfardo314/proxima/proxi/setup"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	cobra.OnInitialize(initConfig)

	initRoot()
	console.Init(rootCmd)
	setup.Init(rootCmd)
}

var (
	configFile string
)

var rootCmd = &cobra.Command{
	Use:   "proxi",
	Short: "a simple CLI for the Proxima project",
	Long: `proxi is a CLI tool for the Proxima project.
It provides:
      - database level access to the Proxima ledger for admin purposes, including genesis creation
      - access to ledger via the Proxima node API. This includes simple wallet functions
`,
	Run: func(cmd *cobra.Command, args []string) {
		_ = cmd.Help()
	},
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	fmt.Printf("++++++ inside init config\n")
	console.Infof("++++++++ config file is: '%s'", configFile)
	if configFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(configFile)
	} else {
		viper.AddConfigPath(".")
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)
		// Search config in current directory with name ".proxi" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigFile(".proxi.yaml")
		viper.SetConfigType("yaml")
	}

	viper.AutomaticEnv() // read in environment variables that match

	if err := viper.ReadInConfig(); err == nil {
		_, _ = fmt.Fprintf(os.Stderr, "Using config profile: %s\n", viper.ConfigFileUsed())
	}
}

func initRoot() {
	fmt.Printf("+++++++++ args = %v\n", os.Args)
	rootCmd = &cobra.Command{
		Use:   "proxi",
		Short: "a simple CLI for the Proxima project",
		Long: `proxi is a CLI tool for the Proxima project.
It provides:
      - database level access to the Proxima ledger for admin purposes, including genesis creation
      - access to ledger via the Proxima node API. This includes simple wallet functions
`,
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
	}

	var mmm string
	rootCmd.PersistentFlags().StringVarP(&mmm, "aflag", "a", "kuku-aflag", "random")
	//err := rootCmd.PersistentFlags().Parse([]string{"aflag"})
	//console.NoError(err)
	fmt.Printf("+++++++++++++ aflag '%s'\n", mmm)
	//viper.BindPFlag("aflag", rootCmd.PersistentFlags().Lookup("aflag"))

	var sss string
	rootCmd.PersistentFlags().StringVarP(&sss, "config", "c", "kuku", "config file (default is .proxi.yaml)")
	fmt.Printf("+++++++++++++ config '%s'\n", sss)
	viper.BindPFlag("config", rootCmd.PersistentFlags().Lookup("config"))
	//fmt.Printf("+++++++++++ get string: '%s'\n", viper.GetString("config"))

}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
