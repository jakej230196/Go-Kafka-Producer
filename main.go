package KafkaProducer
import (
"fmt"
"github.com/confluentinc/confluent-kafka-go/kafka" 
"github.com/prometheus/client_golang/prometheus"
log "github.com/sirupsen/logrus"
"time"
"strings"
"path"
"runtime"
"os"
)
var Delivered bool
var WriteStartTime time.Time

func GetLogFormatter() *log.JSONFormatter {
	Formatter := &log.JSONFormatter{
		CallerPrettyfier: func(f *runtime.Frame) (string, string) {
			return f.Function, fmt.Sprintf("%s:%d", path.Base(f.File), f.Line)
		},
		FieldMap: log.FieldMap{
			log.FieldKeyFile:  "@file",
			log.FieldKeyTime:  "@timestamp",
			log.FieldKeyLevel: "@level",
			log.FieldKeyMsg:   "@message",
			log.FieldKeyFunc:  "@caller",
		},
	}
	Formatter.TimestampFormat = "2006-01-02T15:04:05.999999999Z"
	return Formatter
}

func LogLevel(lvl string) log.Level {
	level := strings.ToLower(lvl)
	switch level {
	case "debug":
		return log.DebugLevel
	case "info":
		return log.InfoLevel
	case "error":
		return log.ErrorLevel
	case "fatal":
		return log.FatalLevel
	default:
		panic(fmt.Sprintf("Log level (%s) is not supported", lvl))
	}
}

func init() {
    os.Setenv("LOG_LEVEL", "info")
	log.SetReportCaller(true)
	Formatter := GetLogFormatter()
	log.SetFormatter(Formatter)
	log.SetLevel(LogLevel(os.Getenv("LOG_LEVEL")))
}

func Start(KafkaServer string, KafkaTopic string, InfoGauge *prometheus.GaugeVec, TotalErrorsWriteGauge *prometheus.GaugeVec, WriteDurationHistogram *prometheus.HistogramVec) chan []byte{
	_Producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": KafkaServer})
	if err != nil {
        log.Fatal("Failed to create producer: ", err, "\n")
	}
	go func() {
		// Set start time for read
		InfoGauge.With(prometheus.Labels{"queue": KafkaTopic, "worker": "producer"}).Set(1)
		for e := range _Producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
                    Delivered = true
                    TotalErrorsWriteGauge.With(prometheus.Labels{"queue": KafkaTopic, "worker": "producer"}).Inc()
					log.Error("Delivery failed: %v\n", ev.TopicPartition.Error, string(ev.Value))
				} else {
                    Delivered = true
					WriteDurationHistogram.With(prometheus.Labels{"queue": KafkaTopic, "worker": "producer"}).Observe(time.Since(WriteStartTime).Seconds())
					log.Info("Delivered message to topic %s [%d] at offset %v\n",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()
    ProducerInputChannel := make(chan []byte, 60000)
    go func(){
        defer close(ProducerInputChannel)
        for {
            Delivered = false
            WriteStartTime = time.Now()
            _Producer.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &KafkaTopic, Partition: kafka.PartitionAny}, Value: <- ProducerInputChannel}
            // wait for message delivery
            for !Delivered {}
        }
    }()
    return ProducerInputChannel
}
