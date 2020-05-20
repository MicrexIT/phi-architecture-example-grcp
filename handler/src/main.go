package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
)

type Event struct {
	Name    string  `json:"name"`
	Version string  `json:"version"`
	Payload Payload `json:"payload"`
}
type Payload struct {
	Product  string `json:"product"`
	Customer string `json:"customer"`
	Visitor  string `json:"visitor"`
}

func mongoClient() func(collectionName string) *mongo.Collection {
	uri, ok := os.LookupEnv("ENTITY_STORE")
	if !ok {
		// default val
		uri = "localhost:27017"
	}
	database, ok := os.LookupEnv("DATABASE")
	if !ok {
		// default val
		database = "entities"
	}

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://"+uri))

	if err != nil {
		log.Fatal(err)
	}

	return func(collectionName string) *mongo.Collection {
		return client.Database(database).Collection(collectionName)
	}
}

func kafkaReader() *kafka.Reader {
	eventStore, ok := os.LookupEnv("EVENT_STORE")
	if !ok {
		eventStore = "localhost:9092"
	}

	topic, ok := os.LookupEnv("TOPIC")
	if !ok {
		topic = "events"
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{eventStore},
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	fmt.Println("returning kafka reader", r)
	return r
}

func HandleMessage(m *kafka.Message) {
	event := decodeMsg(m)
	fmt.Println("decoded message", event)

	dbClient := mongoClient()
	if event.Version != "v1.0" {
		fmt.Println("Wrong version")
		return
	} else if event.Name == "product_bought" {
		// products
		collectionName := "products"
		collection := dbClient(collectionName)

		filter := bson.M{"name": event.Payload.Product}
		update := bson.M{
			"$inc": bson.M{"bought": 1},
		}

		_ = collection.FindOneAndUpdate(context.Background(), filter, update)
		// customers
		collectionName = "customers"
		collection = dbClient(collectionName)

		filter = bson.M{"name": event.Payload.Product}
		update = bson.M{
			"$inc": bson.M{"products": 1},
		}

		_ = collection.FindOneAndUpdate(context.Background(), filter, update)
		fmt.Println("Finished Updating Product bought...")

	} else if event.Name == "product_watched" {
		fmt.Println("product watched!!!")
		collectionName := "products"
		collection := dbClient(collectionName)

		filter := bson.M{"name": event.Payload.Product}

		update := bson.M{
			"$inc": bson.M{"watched": 1},
		}

		res := collection.FindOneAndUpdate(context.Background(), filter, update)
		fmt.Println("Finished Updating Product watched...")
		fmt.Println(res)
	} else {
		// unknown
		fmt.Println("Unknown request...")
	}
}

func decodeMsg(m *kafka.Message) *Event {
	event := Event{}
	err := json.Unmarshal(m.Value, &event)
	if err != nil {
		log.Fatalln(err)
	}
	return &event
}

func main() {
	reader := kafkaReader()
	defer reader.Close()

	fmt.Println("start consuming ...")
	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("start Reading msg ...")
		fmt.Print(string(msg.Value))
		HandleMessage(&msg)
	}

}
