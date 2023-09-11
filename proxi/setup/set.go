package setup

import (
	"github.com/lunfardo314/proxima/proxi/console"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func initConfigSetCmd() *cobra.Command {
	setConfigCmd := &cobra.Command{
		Use:   "set <key> <value>",
		Short: "Set a configuration value",
		Args:  cobra.ExactArgs(2),
		Run:   runConfigSetCmd,
	}
	return setConfigCmd
}

func runConfigSetCmd(cmd *cobra.Command, args []string) {
	v := args[1]
	if args[0] == "private_key" {
		console.Fatalf("use 'proxi set_private_key [<key>]' command to set a private key")
	}
	switch v {
	case "true":
		_set(args[0], true)
	case "false":
		_set(args[0], false)
	default:
		_set(args[0], v)
	}
}

func _set(key string, value interface{}) {
	viper.Set(key, value)
	console.NoError(viper.WriteConfig())
}
