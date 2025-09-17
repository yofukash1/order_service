package orders

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
	"wb_tech/L0/pkg/kafka_client"

	orders "wb_tech/L0/internal/orders"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaHandler struct {
	r orders.Repository
}

func NewKafkaHandler(r orders.Repository) kafka_client.Handler {
	return &KafkaHandler{r: r}
}

func (h *KafkaHandler) HandleMessage(message []byte, topic kafka.TopicPartition, consumerNumber int) error {
	log.Printf("Consumer #%d Message from kafka with offset %d on partition %d\n",
		consumerNumber, topic.Offset, topic.Partition)

	var order orders.Order
	if err := json.Unmarshal(message, &order); err != nil {
		log.Printf("Failed to unmarshal Kafka message: %v", err)
		return fmt.Errorf("failed to unmarshal order: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := h.r.CreateOrder(ctx, &order); err != nil {
		log.Printf("Failed to create order %s: %v", order.OrderUID, err)
		return fmt.Errorf("failed to create order: %w", err)
	}

	log.Printf("Successfully processed order %s from Kafka\n", order.OrderUID)
	return nil
}
