package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/prometheus/common/model"
)

// Client Generic
type Client struct {
	DB     *gorm.DB
	config string
}

// Metric Model
type Metric struct {
	Timestamp time.Time
	Metric    string
	Value     string
}

func (c *Client) Write(samples model.Samples) error {
	concurrency := 10
	in := make(chan *Metric)
	out := make(chan error)

	for i := 0; i < concurrency; i++ {
		go c.writeToDB(in, out)
	}

	go func() {
		for _, metric := range samples {
			in <- &Metric{
				Timestamp: metric.Timestamp.Time(),
				Metric:    metric.Metric.String(),
				Value:     metric.Value.String(),
			}
		}
		close(in)
	}()

	for err := range out {
		if err != nil {
			fmt.Println(err.Error())
			break
		}
	}
	return nil
}

func (c *Client) writeToDB(in chan *Metric, out chan error) {
	tx := c.DB.Begin()
	defer tx.Rollback()
	for metric := range in {
		if metric == nil {
			out <- fmt.Errorf("Metric can't be nil %v", metric)
		}
		if err := tx.Save(&metric).Error; err != nil {
			out <- err
		}
	}
	if err := tx.Commit().Error; err != nil {
		out <- err
	}
	out <- nil
}

// Name for Cockroach Client
func (c Client) Name() string {
	return "CockroachDB"
}

// GetInstance of DB
func (c Client) GetInstance() *gorm.DB {
	return c.DB
}

func newCockroachClient() *Client {
	user := os.Getenv("COCKROACH_USER")
	password := os.Getenv("COCKROACH_PASS")
	host := os.Getenv("HOST")
	port := os.Getenv("PORT")
	database := os.Getenv("DB")
	sslrootcert := os.Getenv("SSL_ROOT_CERT")
	sslkey := os.Getenv("SSL_KEY")
	sslcert := os.Getenv("SSL_CERT")
	connString := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s?ssl=true&sslmode=require&sslrootcert=%s&sslkey=%s&sslcert=%s", user, password, host, port, database, sslrootcert, sslkey, sslcert)
	db, err := gorm.Open("postgres", connString)
	if err != nil {
		log.Fatal(err)
	}
	db.LogMode(false)
	db.AutoMigrate(&Metric{})
	return &Client{
		DB:     db,
		config: "",
	}
}
