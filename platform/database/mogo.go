package database

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

const mongoConnectTimeout = 10 * time.Second

type MongoInstance struct {
	Client *mongo.Client
	DB     *mongo.Database
}

var (
	mongoOnce sync.Once
	MI        *MongoInstance
)

// NewMongoConnection initializes the singleton MongoDB connection (with timeout + ping).
func NewMongoConnection(url string) (*MongoInstance, error) {
	var initErr error

	mongoOnce.Do(func() {
		log.Println("Mongodb database connecting...")

		cs, err := connstring.ParseAndValidate(url)
		if err != nil {
			initErr = errors.Wrap(err, "couldn't parse mongo url")
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), mongoConnectTimeout)
		defer cancel()

		client, err := mongo.Connect(ctx, options.Client().ApplyURI(url))
		if err != nil {
			initErr = errors.Wrap(err, "connect mongodb failed")
			return
		}

		if err := client.Ping(ctx, nil); err != nil {
			initErr = errors.Wrap(err, "ping mongodb failed")
			return
		}

		MI = &MongoInstance{Client: client, DB: client.Database(cs.Database)}
		log.Println("Mongodb database connected.")
	})

	if initErr != nil {
		return nil, initErr
	}
	return MI, nil
}
