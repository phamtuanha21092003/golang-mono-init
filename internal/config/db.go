package config

import (
	"os"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

// DB holds the DB configuration
type DB struct {
	*viper.Viper

	SqlUri      string
	MongoUri    string
	Debug       bool
	RabbitMQUri string
	RedisUri    string
}

var db = &DB{
	Viper: viper.New(),
}

// DBCfg returns the default DB configuration
func DBCfg() *DB {
	return db
}

// LoadDBInternalConsumerCfg loads DB configuration
func LoadDBInternalConsumerCfg(isDebug bool) error {
	db.SqlUri = os.Getenv("SQL_URI")
	db.RabbitMQUri = os.Getenv("RABBITMQ_URI")
	db.Debug = isDebug

	if db.SqlUri == "" {
		return errors.New("SQL_URI is required")
	}

	if db.RabbitMQUri == "" {
		return errors.New("RABBITMQ_URI is required")
	}

	return nil
}
