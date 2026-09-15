package config

import (
	"path"
	"runtime"

	"github.com/spf13/viper"
)

type (
	InternalConsumerCfg struct {
		*App
		*DB

		*viper.Viper
	}
)

var (
	internalConsumerCfg *InternalConsumerCfg = &InternalConsumerCfg{
		Viper: viper.New(),
	}
)

func NewInternalConsumerCfg() (*InternalConsumerCfg, error) {
	err := LoadAppCfg()
	if err != nil {
		return nil, err
	}
	internalConsumerCfg.App = GetAppCfg()

	err = LoadDBInternalConsumerCfg(internalConsumerCfg.App.Debug)
	if err != nil {
		return nil, err
	}
	internalConsumerCfg.DB = DBCfg()

	_, filename, _, _ := runtime.Caller(0)

	folderPath := path.Join(path.Dir(filename), "../../setting")
	internalConsumerCfg.AddConfigPath(folderPath)
	// add in container
	internalConsumerCfg.AddConfigPath("/app/settings")

	internalConsumerCfg.SetConfigName("internal-consumer")

	internalConsumerCfg.SetConfigType("toml")

	internalConsumerCfg.AutomaticEnv()

	if err := internalConsumerCfg.ReadInConfig(); err != nil {
		return nil, err
	}

	return internalConsumerCfg, nil
}
