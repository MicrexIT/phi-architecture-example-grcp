package main

import (
	"context"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"net"

	schema "github.com/micrexIT/phi-architecture-example-protobuf"
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
	inspect := func(session neo4j.Session ) error {
		var err error
		result, err := session.Run(`MATCH (p:Person)-[b:BOUGHT]-(pp:Product) WHERE NOT p.name = "anonymous" RETURN p.name as name , sum(b.items) as products`, map[string]interface{}{})
		if err != nil {
			return err // handle error
		}
		for result.Next() {
			err = result.Err()
			if err != nil {
				return err
			}
			record := map[string]interface{}{}
			for _, key := range result.Record().Keys() {
				record[key], _ = result.Record().Get(key)
				fmt.Println(record[key])
			}
			product := schema.Product{}
			product.Name = record["name"].(string)
			product.Bought = record["bought"].(int64)
			product.Watched = record["watched"].(int64)
			if err := stream.Send(&product); err != nil {
				fmt.Println("Error", err)
				return err
			}
		}
		return nil
	}
	return Neo4jClient(inspect)

}

func (i *InspectorServer) InspectCustomer(_ *schema.Empty, stream schema.Inspector_InspectCustomerServer) error {
	inspect := func(session neo4j.Session ) error {
		var err error
		result, err := session.Run(`MATCH (p:Person)-[b:BOUGHT]-(pp:Product) WHERE NOT p.name = "anonymous" RETURN p.name as name , sum(b.items) as products`, map[string]interface{}{})
		if err != nil {
			return err // handle error
		}
		for result.Next() {
			err = result.Err()
			if err != nil {
				return err
			}
			record := map[string]interface{}{}
			for _, key := range result.Record().Keys() {
				record[key], _ = result.Record().Get(key)
				fmt.Println(record[key])
			}
			customer := schema.Customer{}
			customer.Name = record["name"].(string)
			customer.Products = record["products"].(int64)
			if err := stream.Send(&customer); err != nil {
				fmt.Println("Error", err)
				return err
			}
		}
		return nil
	}
    return Neo4jClient(inspect)

}


func Neo4jClient(foo func (session neo4j.Session) error) error {
	driver, err := neo4j.NewDriver("bolt://neo4j:7687", neo4j.BasicAuth("neo4j", "qwerqwer", ""))
	if err != nil {
		return err // handle error
	}
	defer driver.Close()

	session, err := driver.Session(neo4j.AccessModeRead)
	if err != nil {
		return err
	}
	defer session.Close()
	return foo(session)
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

