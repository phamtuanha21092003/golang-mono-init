package config

import (
	"os"
	"strconv"

	"github.com/pkg/errors"
)

type (
	App struct {
		Name        string
		Version     string
		Host        string
		Port        int
		Environment string
		Debug       bool
	}
)

const defaultAppPort = 3000

var (
	app = &App{}
)

func GetAppCfg() *App {
	return app
}

func LoadAppCfg() error {
	app.Name = os.Getenv("APP_NAME")
	app.Environment = os.Getenv("APP_ENV") // production or development
	app.Host = os.Getenv("APP_HOST")
	app.Debug, _ = strconv.ParseBool(os.Getenv("APP_DEBUG"))
	app.Version = "1.0.1"

	if port := os.Getenv("APP_PORT"); port != "" {
		parsed, err := strconv.Atoi(port)
		if err != nil {
			return errors.Wrap(err, "invalid APP_PORT")
		}
		app.Port = parsed
	} else {
		app.Port = defaultAppPort
	}

	return nil
}
