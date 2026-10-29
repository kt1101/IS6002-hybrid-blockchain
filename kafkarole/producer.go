package kafkarole

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func NewProducer(serverAddr, topic string) (*kafka.Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": serverAddr})
	if err != nil {
		return nil, err
	}
	return p, nil
}
