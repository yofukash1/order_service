package kafka_client

import (
	"log"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	sessionTimeout = 7000 // ms
	noTimeout      = -1
)

type Handler interface {
	HandleMessage(message []byte, topic kafka.TopicPartition, consumerNumber int) error
}

type Consumer struct {
	consumer       *kafka.Consumer
	handler        Handler
	stop           bool
	consumerNumber int
}

func NewConsumer(handler Handler, address []string, topic string, consumerGroup string, consumerNumber int) (*Consumer, error) {
	cfg := &kafka.ConfigMap{
		"bootstrap.servers":        strings.Join(address, ","),
		"group.id":                 consumerGroup,
		"session.timeout.ms":       sessionTimeout,
		"enable.auto.offset.store": false,
		"enable.auto.commit":       true,
		"auto.commit.interval.ms":  5000,
		"auto.offset.reset":        "earliest",
	}
	c, err := kafka.NewConsumer(cfg)
	if err != nil {
		return nil, err
	}
	if err = c.Subscribe(topic, nil); err != nil {
		return nil, err
	}
	return &Consumer{consumer: c, handler: handler, consumerNumber: consumerNumber}, nil
}

func (c *Consumer) Start() {
	for {
		if c.stop {
			break
		}
		kafkaMsg, err := c.consumer.ReadMessage(noTimeout)
		if err != nil {
			log.Println("Error while reading kafka message")
		}
		if kafkaMsg == nil {
			continue
		}
		if err := c.handler.HandleMessage(kafkaMsg.Value, kafkaMsg.TopicPartition, c.consumerNumber); err != nil {
			log.Println("Erro while handling the message")
		}
		if _, err := c.consumer.StoreMessage(kafkaMsg); err != nil {
			log.Println("Erro while storing the message offset")
			continue
		}
	}

}

func (c *Consumer) Stop() error {
	c.stop = true
	if _, err := c.consumer.Commit(); err != nil {
		return err
	}
	log.Println("Commited offset")
	return c.consumer.Close()
}
