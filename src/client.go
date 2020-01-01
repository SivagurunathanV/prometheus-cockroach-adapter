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

func (c *Client) saveMetric(db *gorm.DB, metric *Metric) error {
	fmt.Printf("%v\n", *metric)
	if metric == nil {
		return fmt.Errorf("Metric can't be nil %v", metric)
	}

	if err := db.Save(&metric).Error; err != nil {
		return err
	}
	return nil
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
	fmt.Println(connString)
	const addr = "postgresql://maxroach:root@localhost:26257/prometheus_backup?ssl=true&sslmode=require&sslrootcert=$HOME/certs/ca.crt&sslkey=$HOME/certs/client.maxroach.key&sslcert=certs/client.maxroach.crt"
	db, err := gorm.Open("postgres", addr)
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
