package main

import (
	"context"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"net"

	schema "github.com/micrexIT/phi-architecture-example-protobuf"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"

	// "github.com/golang/protobuf/proto"
	"log"
	"os"
)

type InspectorServer struct {
	schema.UnimplementedInspectorServer
}

func main() {
	fmt.Println("Service booting...")
	port := 50051
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	schema.RegisterInspectorServer(grpcServer, newServer())
	fmt.Println("serving grcp")
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve grpc: %v", err)
	}

}

func newServer() *InspectorServer {
	s := &InspectorServer{}
	return s
}

func (i *InspectorServer) InspectProduct(_ *schema.Empty, stream schema.Inspector_InspectProductServer) error {
	dbClient := mongoClient()
	collectionName := "products"
	collection := dbClient(collectionName)
	cursor, err := collection.Find(context.Background(), bson.D{})
	if err != nil {
		fmt.Println("Error", err)
		return err
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		var decoded schema.Product
		err := cursor.Decode(&decoded)
		if err != nil {
			fmt.Println("Error", err)
			return err
		}
		if err := stream.Send(&decoded); err != nil {
			return err
		}
	}
	return nil
}

//
// func (i *InspectorServer) InspectCustomer(_ *schema.Empty, stream schema.Inspector_InspectCustomerServer) error {
// 	dbClient := mongoClient()
// 	collectionName := "customers"
// 	collection := dbClient(collectionName)
// 	cursor, err := collection.Find(context.Background(), bson.D{})
// 	if err != nil {
// 		fmt.Println("Error", err)
// 		return err
// 	}
// 	defer cursor.Close(context.Background())
//
// 	for cursor.Next(context.Background()) {
// 		var decoded schema.Customer
// 		if err := cursor.Decode(&decoded); err != nil {
// 			fmt.Println("Error", err)
// 			return err
// 		}
// 		if err := stream.Send(&decoded); err != nil {
// 			fmt.Println("Error", err)
// 			return err
// 		}
// 	}
// 	return nil
// }
func (i *InspectorServer) InspectCustomer(_ *schema.Empty, stream schema.Inspector_InspectCustomerServer) error {

	// 1. Open connection with neo4j
	fmt.Println("in neo4j client")
	driver, err := neo4j.NewDriver("bolt://localhost:7687", neo4j.BasicAuth("neo4j", "qwerqwer", ""))
	if err != nil {
		return err // handle error
	}
	// handle driver lifetime based on your application lifetime requirements
	// driver's lifetime is usually bound by the application lifetime, which usually implies one driver instance per application
	defer driver.Close()
	fmt.Println("got Driver")


	session, err := driver.Session(neo4j.AccessModeRead)
	if err != nil {
		return err
	}

	defer session.Close()
	fmt.Println("Got Session")
	// 2. Request customers nodes and their edges with products
	result, err := session.Run(`MATCH (p:Person)-[b:BOUGHT]-(pp:product) WHERE p.name != "anonymous" RETURN p.name as name , sum(b.items)`,map[string]interface{}{})
	if err != nil {
		return err // handle error
	}

	fmt.Println("handling next")

	for result.Next() {
		// split in two: 1 create array with customers
		// 2. stream through array
		fmt.Println("inside record.Next()")
		customer := &schema.Customer{}
		// (*customer).Name = result.Record().GetByIndex(0).(string)
		// (*customer).Products += result.Record().GetByIndex(1).(int64)
		fmt.Println(*customer)
		if err := stream.Send(customer); err != nil {
			fmt.Println("Error", err)
			return err
		}

	}
	return nil
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

