package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	proto "github.com/MicrexIT/phi-architecture-example-protobuf"
	"log"
	"net/http"
	"os"
	"strings"
)

type Customer struct {
	Name     string `json:"name"`
	Products int64  `json:"products"`
}

type Product struct {
	Name    string `json:"name"`
	Watched int64  `json:"watched"`
	Bought  int64  `json:"bought"`
}

func main() {
	proto.
	http.ListenAndServe(":80", http.HandlerFunc(handler))
	fmt.Println("server started")
}
func handler(w http.ResponseWriter, r *http.Request) {
	path := strings.Split(r.URL.Path[1:], "/")
	switch path[1] {
	case "product":
		inspectProduct(w)

	case "customer":
		inspectCustomer(w)

	default:
		http.NotFound(w, r)
	}
}
func inspectProduct(w http.ResponseWriter) {
	// products
	dbClient := mongoClient()
	collectionName := "products"
	collection := dbClient(collectionName)
	resp := Product{}
	inspector(collection, resp, w)

}

func inspector(collection *mongo.Collection, resp interface{}, w http.ResponseWriter) {
	cursor, err := collection.Find(context.Background(), bson.D{})
	if err != nil {
		fmt.Println("Error", err)
		return
	}
	defer cursor.Close(context.Background())
	w.Header().Set("Content-Type", "application/json")
	for cursor.Next(context.Background()) {
		// To decode into a struct, use cursor.Decode()
		err := cursor.Decode(&resp)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(resp)

		//raw := cursor.Current
		fmt.Fprintf(w, "%s\n", resp)
	}
}

func inspectCustomer(w http.ResponseWriter) {
	dbClient := mongoClient()
	collectionName := "customers"
	collection := dbClient(collectionName)
	resp := Customer{}
	inspector(collection, resp, w)
}

func mongoClient() func(collectionName string) *mongo.Collection {

	uri, ok := os.LookupEnv("ENTITY_STORE")
	if !ok {
		uri = "localhost:27017"
	}
	database, ok := os.LookupEnv("DATABASE")
	if !ok {
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


