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

type MongoWriter struct {
	CollectionName string
	Filter         bson.M
	Update         bson.M
	Insert         interface{}
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
	switch event.Name {
	case "product_bought":
		increaseProductBought(event) //split in two: more meaningful; updateCust + updateProduct
		increaseCustomerProduct(event)
	case "product_watched":
		increaseProductWatched(event)
	default:
		fmt.Println("Unknown event: \n")
		fmt.Printf("%v", *event)
	}
}

func increaseProductWatched(event *Event) {
	collectionName := "products"
	insert := struct {
		Name    string `json:"name"`
		Watched int64  `json:"watched"`
		Bought  int64  `json:"bought"`
	}{Name: event.Payload.Product, Watched: 1, Bought: 0}
	filter := bson.M{"name": event.Payload.Product}

	update := bson.M{
		"$inc": bson.M{"watched": 1},
	}

	mw := MongoWriter{
		CollectionName: collectionName,
		Filter:         filter,
		Update:         update,
		Insert:         insert,
	}
	mw.write()

}

func increaseProductBought(event *Event) {
	collectionName := "products"

	filter := bson.M{"name": event.Payload.Product}
	update := bson.M{
		"$inc": bson.M{"bought": 1},
	}
	insert := struct {
		Name    string `json:"name"`
		Watched int64  `json:"watched"`
		Bought  int64  `json:"bought"`
	}{Name: event.Payload.Product, Watched: 0, Bought: 1}

	mw := MongoWriter{
		CollectionName: collectionName,
		Filter:         filter,
		Update:         update,
		Insert:         insert,
	}
	mw.write()
}

func increaseCustomerProduct(event *Event) {
	collectionName := "customers"
	filter := bson.M{"name": event.Payload.Customer}
	update := bson.M{
		"$inc": bson.M{"products": 1},
	}
	insert := struct {
		Name     string `json:"name"`
		Products int64  `json:"products"`
	}{Name: event.Payload.Customer, Products: 1}

	mw := MongoWriter{
		CollectionName: collectionName,
		Filter:         filter,
		Update:         update,
		Insert:         insert,
	}
	mw.write()
}

func (mw *MongoWriter) write() {

	dbClient := mongoClient()

	collection := dbClient(mw.CollectionName)

	var updatedDocument bson.M
	err := collection.FindOneAndUpdate(context.Background(), mw.Filter, mw.Update).Decode(&updatedDocument)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			fmt.Println(err)
			_, err = collection.InsertOne(context.Background(), mw.Insert)
			if err != nil {
				fmt.Println(err)
				return
			}
		}
	}
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

func decodeMsg(m *kafka.Message) *Event {
	event := Event{}
	err := json.Unmarshal(m.Value, &event)
	if err != nil {
		log.Fatalln(err)
	}
	return &event
}
