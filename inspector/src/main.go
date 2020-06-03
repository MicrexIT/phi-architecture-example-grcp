package main

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"net"

	schema "github.com/micrexIT/phi-architecture-example-protobuf"
	"google.golang.org/grpc"

	// "github.com/golang/protobuf/proto"
	"log"
)

type InspectorServer struct {
	schema.UnimplementedInspectorServer
}

type Record map[string]interface{}

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
	query :=`MATCH (:Person)-[b:BOUGHT|:WATCHED]->(pp:Product) RETURN pp.name as name , sum(b.items) as bought, count(b) - count(b.items)  as watched`
	inspect := func(record Record) error {
		product := schema.Product{}
		product.Name = record["name"].(string)
		product.Bought = record["bought"].(int64)
		product.Watched = record["watched"].(int64)

		if err := stream.Send(&product); err != nil {
			fmt.Println("Error", err)
			return err
		}
		return nil
	}

	return Neo4jClient(query, inspect)

}

func (i *InspectorServer) InspectCustomer(_ *schema.Empty, stream schema.Inspector_InspectCustomerServer) error {
	query :=`MATCH (p:Person)-[b:BOUGHT]->(pp:Product) RETURN p.name as name , sum(b.items) as products`
	inspect := func(record Record) error {
			customer := schema.Customer{}
			customer.Name = record["name"].(string)
			customer.Products = record["products"].(int64)
			if err := stream.Send(&customer); err != nil {
				fmt.Println("Error", err)
				return err
			}
		return nil
	}
	return Neo4jClient(query, inspect)
}

func Neo4jClient(query string, job func(record Record) error) error {
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
	result, err := session.Run(query, map[string]interface{}{})
	if err != nil {
		return err // handle error
	}
	for result.Next() {
		err = result.Err()
		if err != nil {
			return err
		}
		record := Record{}
		for _, key := range result.Record().Keys() {
			record[key], _ = result.Record().Get(key)
			fmt.Println(record[key])
		}

		if err := job(record); err != nil {
			fmt.Println("Error", err)
			return err
		}
	}
	return nil
}
