package database

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	//postgres driver
	_ "github.com/lib/pq"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// DbConn : DbConn
var (
	GormConn *GormDatabase
	onceGorm sync.Once
)

// Database : database
type GormDatabase struct {
	*gorm.DB
}

// InitDatabaseConn : Init new database connection
func InitDatabaseConn(uri string) *GormDatabase {
	onceGorm.Do(func() {
		db, err := gorm.Open(postgres.Open(uri), &gorm.Config{
			SkipDefaultTransaction: true,
			PrepareStmt:            true,
			Logger: logger.New(
				log.New(os.Stdout, "\r\n", log.LstdFlags),
				logger.Config{
					SlowThreshold:             time.Second,
					LogLevel:                  logger.Info,
					IgnoreRecordNotFoundError: false,
					Colorful:                  true,
				},
			),
		})
		sqlDB, err := db.DB()
		if err != nil {
			panic(fmt.Sprintf("Unable to connection to database: %v\n", err))
		}
		sqlDB.SetMaxIdleConns(50)
		sqlDB.SetMaxOpenConns(10)
		sqlDB.SetConnMaxLifetime(30 * time.Minute)
		sqlDB.SetConnMaxIdleTime(5 * time.Minute)
		GormConn = &GormDatabase{db}
	})
	return GormConn

}

// PrePing : PrePing
func (db GormDatabase) PrePing() error {
	var err error
	for range [3]int{} {
		if err = db.DB.Raw("SELECT 1").Error; err != nil {
			fmt.Println("Fail to connect database, retry after 1s")
			time.Sleep(1 * time.Second)
		}
	}
	return err
}

// CreateTestDatabase : CreateTestDatabase
func CreateTestDatabase(databaseURL string) {
	u, err := url.Parse(databaseURL)
	fmt.Printf(databaseURL)
	user := u.User.Username()
	password, _ := u.User.Password()
	databaseName := strings.ReplaceAll(u.Path, "/", "")
	dsn := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		user, password, u.Host, "postgres")

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	_, err = db.Exec("DROP DATABASE IF EXISTS " + databaseName + " WITH (FORCE);")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("CREATE DATABASE " + databaseName)
	if err != nil {
		panic(err)
	}
}

// RecordNotFound check if returning ErrRecordNotFound error
func (s *GormDatabase) RecordNotFound(err error) bool {
	if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
		return true
	}
	return false
}

// RecordNotFound check if returning ErrRecordNotFound error
func (s *GormDatabase) ErrorQuery(err error) bool {
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return true
	}
	return false
}
