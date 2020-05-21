package main

import (
	"context"
	//"flag"
	"fmt"
	schema "github.com/micrexIT/phi-architecture-example-protobuf"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	//"google.golang.org/grpc/credentials"
	"net"

	//grpc "github.com/phiskills/grpc-api.go"
	"google.golang.org/grpc"
	//"github.com/golang/protobuf/proto"
	"log"
	"os"
)

type InspectorServer struct{
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
	grpcServer.Serve(lis)


}

func newServer() *InspectorServer {
	s := &InspectorServer{}
	return s
}

func (i *InspectorServer) InspectProduct(ctx context.Context, _ *schema.Empty) (*schema.ProductMany, error){
	fmt.Println("InspectorServer")
	dbClient := mongoClient()
	collectionName := "products"
	collection := dbClient(collectionName)
	resp := schema.ProductMany{}
	cursor, err := collection.Find(ctx, bson.D{})
	if err != nil {
		fmt.Println("Error", err)
		return nil, err
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {

		var decoded schema.Product
		err := cursor.Decode(&decoded)
		if err != nil {
			fmt.Println("Error", err)
			return nil, err
		}
		fmt.Println(decoded)
		resp.ProductMany = append(resp.ProductMany, &decoded)
		fmt.Println(resp)
	}
	//inspect(collection, resp)
	return &resp, nil
}

func (i *InspectorServer) InspectCustomer(ctx context.Context, req *schema.Empty) (*schema.CustomerMany, error){
	fmt.Println("InspectorServer")
	dbClient := mongoClient()
	collectionName := "customers"
	collection := dbClient(collectionName)
	resp := schema.CustomerMany{}
	cursor, err := collection.Find(ctx, bson.D{})
	if err != nil {
		fmt.Println("Error", err)
		return nil, err
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var decoded schema.Customer
		err := cursor.Decode(&decoded)
		if err != nil {
			fmt.Println("Error", err)
			return nil, err
		}

		fmt.Println(decoded.GetName())
		resp.CustomerMany = append(resp.CustomerMany, &decoded)
		fmt.Println(resp.GetCustomerMany())
	}
	//inspect(collection, resp)
	return &resp, nil
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


