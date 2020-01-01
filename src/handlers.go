package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/jinzhu/gorm"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

var (
	httpTotalRequests = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "http_total_requests",
			Help: "Count of HTTP Requests",
		},
	)
	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_ms",
			Help:    "Duration of HTTP request in milliseconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"path"},
	)
)

// Writer Interface
type writer interface {
	Write(samples model.Samples) error
	Name() string
	GetInstance() *gorm.DB
}

// Registering the Metrics Counter for http request
func init() {
	prometheus.MustRegister(httpTotalRequests)
	prometheus.MustRegister(httpRequestDuration)
}

func main() {
	writer := buildClient()
	defer writer.GetInstance().Close()
	http.Handle("/write", timeHandler("write", write(writer)))
	err := http.ListenAndServe(":8000", nil)
	if err != nil {
		log.Fatalf("Listen Failure %s", err)
		os.Exit(1)
	}
}
func buildClient() writer {
	return newCockroachClient()
}

func write(w writer) http.Handler {
	log.Println("Welcome")
	return http.HandlerFunc(func(reponseWriter http.ResponseWriter, r *http.Request) {
		compressedData, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Error while reading compressed data %v", err.Error())
			http.Error(reponseWriter, err.Error(), http.StatusInternalServerError)
			return
		}

		bufData, err := snappy.Decode(nil, compressedData)

		if err != nil {
			http.Error(reponseWriter, err.Error(), http.StatusBadRequest)
		}

		var request prompb.WriteRequest

		if err := proto.Unmarshal(bufData, &request); err != nil {
			http.Error(reponseWriter, err.Error(), http.StatusBadRequest)
		}

		samples := convertProtoToSamples(&request)
		err = writeSamples(w, samples)
		if err != nil {
			http.Error(reponseWriter, err.Error(), http.StatusInternalServerError)
		}
		reponseWriter.Header().Add("Siva", "Guru")
	})
}

func convertProtoToSamples(request *prompb.WriteRequest) model.Samples {
	var samples model.Samples
	for _, ts := range request.Timeseries {
		// create metrics
		metric := make(model.Metric, len(ts.Labels))
		// copy the metric map
		for _, label := range ts.Labels {
			metric[model.LabelName(label.Name)] = model.LabelValue(label.Value)
		}
		// Add to samples
		for _, sample := range ts.Samples {
			samples = append(samples, &model.Sample{
				Metric:    metric,
				Value:     model.SampleValue(sample.Value),
				Timestamp: model.Time(sample.Timestamp),
			})
		}
	}
	return samples
}

func writeSamples(w writer, samples model.Samples) error {
	w.Write(samples)
	return nil
}

func timeHandler(path string, handler http.Handler) http.Handler {
	f := func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		handler.ServeHTTP(w, r)
		duration := time.Since(start).Nanoseconds() / int64(time.Millisecond)
		httpRequestDuration.WithLabelValues(path).Observe(float64(duration))
		w.Header().Set("Content-Length", "0")
	}
	return http.HandlerFunc(f)
}
