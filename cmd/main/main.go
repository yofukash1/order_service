package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"wb_tech/L0/internal/cacher"
	orders2 "wb_tech/L0/internal/orders"
	orders "wb_tech/L0/internal/orders/db"
	orders_kafka "wb_tech/L0/internal/orders/kafka"
	"wb_tech/L0/pkg/database/postgres"
	"wb_tech/L0/pkg/kafka_client"
	"wb_tech/L0/pkg/redis"

	"github.com/julienschmidt/httprouter"
)

const (
	topic         = "my-topic"
	consumerGroup = "my-consumer-group"
)

var addresses = []string{"localhost:9091", "localhost:9092", "localhost:9093"}

func main() {
	// router
	router := httprouter.New()

	ctx := context.Background()

	// postgres
	postgreSQLClient, err := postgres.NewClient(ctx, 3, "user", "password", "localhost", "5432", "orderservice")

	if err != nil {
		log.Fatalf("Failed to initialize postgres client: %v\n", err)
	}

	// repository
	repository := orders.NewRepository(postgreSQLClient)

	// redis + cacher
	redisClient, err := redis.NewClient()
	if err != nil {
		log.Fatalf("Failed to initialize redis client: %v\n", err)
	}
	defer redisClient.Close()
	cacher := cacher.NewCacher(redisClient)

	// handlers
	ordersHandler := orders2.NewHandler(repository, cacher)
	ordersHandler.Register(router)

	// kafka
	h := orders_kafka.NewKafkaHandler(repository)

	start(router, h)
}

func start(router *httprouter.Router, h kafka_client.Handler) {
	var listener net.Listener
	var listenErr error

	listener, listenErr = net.Listen("tcp", fmt.Sprintf("%s:%s", "localhost", "5612"))
	if listenErr != nil {
		log.Fatalf("Failed to start server: %v\n", listenErr)
	}
	log.Printf("Listening on %s\n", fmt.Sprintf("%s:%s", "localhost", "5612"))

	server := &http.Server{
		Handler:      router,
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	c1, err := kafka_client.NewConsumer(h, addresses, topic, consumerGroup, 1)
	if err != nil {
		log.Fatalln(err)
	}
	c2, err := kafka_client.NewConsumer(h, addresses, topic, consumerGroup, 2)
	if err != nil {
		log.Fatalln(err)
	}
	c3, err := kafka_client.NewConsumer(h, addresses, topic, consumerGroup, 3)
	if err != nil {
		log.Fatalln(err)
	}

	// Запускаем consumers в горутинах
	go c1.Start()
	go c2.Start()
	go c3.Start()

	// Канал для graceful shutdown
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v\n", err)
		}
	}()

	log.Println("Server and Kafka consumers started successfully")

	<-shutdownChan
	log.Println("Shutting down gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("HTTP server shutdown error: %v\n", err)
	}

	if err := c1.Stop(); err != nil {
		log.Printf("Consumer 1 stop error: %v\n", err)
	}
	if err := c2.Stop(); err != nil {
		log.Printf("Consumer 2 stop error: %v\n", err)
	}
	if err := c3.Stop(); err != nil {
		log.Printf("Consumer 3 stop error: %v\n", err)
	}

	log.Println("Server and consumers stopped gracefully")
}
