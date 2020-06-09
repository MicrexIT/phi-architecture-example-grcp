package main

import (
	"fmt"
	"github.com/MicrexIT/neo4j-driver-client"
	"github.com/gin-gonic/gin"
	"io"
	// "io/ioutil"
	"log"
	"os"
)

// MovieResult is the result of moves when searching
type MovieResult struct {
	Movie `json:"movie"`
}

// Movie is a movie
type Customer struct {
	Name     string `json:"name"`
	Products int64  `json:"products"`
}
type Product struct {
	Name    string `json:"name"`
	Bought  int64  `json:"bought"`
	Watched int64  `json:"watched"`
}

// Movie is a movie
type Movie struct {
	Released int      `json:"released"`
	Id    string   `json:"Id,omitempty"`
	Tagline  string   `json:"tagline,omitempty"`
	Cast     []Person `json:"cast,omitempty"`
}

// Person is a person in a movie
type Person struct {
	Job  string   `json:"job"`
	Role []string `json:"role"`
	Name string   `json:"name"`
}

// GraphResponse is the graph response
// TODO: RENAME
type GraphResponse struct {
	Nodes []Node `json:"nodes"`
	//TODO: RENAME EDGES
	Edges []Edge `json:"edges"`
}

// Node is the graph response node
type Node struct {
	Id string `json:"id"`
	Label string `json:"label"`
}

// Link is the graph response link
type Edge struct {
	Source int `json:"source"`
	Target int `json:"target"`
	Meta interface{} `json:"meta"`
	Id int `json:"id"`
	// TODO:add label
	// TODO: add id as an index
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	r := gin.Default()
	r.Use(CORSMiddleware())
	r.Use(gin.Logger())
	r.GET("/graph", func(c *gin.Context) {

		url, username, password := environmentVariables()
		client := neo4j.NewClient(
			url,
			username,
			password,
		)

		product := Product{}
		customer := Customer{}
		graphResp := GraphResponse{}

		query := `
			MATCH (p:Product)<-[w]-(c:Person)
			RETURN p.name as product, c.name as customer, w as meta`
		job := func(record neo4j.Record) error {
			product.Name = record["product"].(string)
			customer.Name = record["customer"].(string)
            productIndex := graphResp.findOrCreateNode(product.Name, "product")
			customerIndex := graphResp.findOrCreateNode(customer.Name, "customer")
			graphResp.Edges = append(graphResp.Edges, Edge{Source: customerIndex, Target: productIndex, Meta: record["meta"], Id: len(graphResp.Nodes) -1})
			fmt.Println("sending graphResp:")
			fmt.Println(graphResp)

			return nil
		}

		err := client.Read(query, job)
		c.JSON(200, graphResp)
		if err != nil && err != io.EOF {
			log.Println("error querying graph:", err)
			return
		} else if len(graphResp.Nodes) == 0 {
			fmt.Println("no nodes")
			return
		}

	})
	r.Run(":8080")

}

func (g GraphResponse) findOrCreateNode(name string, label string) int {
	targetIndex := -1
	for i, node := range g.Nodes {
		if name == node.Id && node.Label == label {
			targetIndex = i
			break
		}
	}

	if targetIndex == -1 {
		g.Nodes = append(g.Nodes, Node{Id: name, Label: label})
		targetIndex = len(g.Nodes) - 1
	}

	return targetIndex
}

func interfaceSliceToString(s []interface{}) []string {
	o := make([]string, len(s))
	for idx, item := range s {
		o[idx] = item.(string)
	}
	return o
}


func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

func environmentVariables() (boltUrl string, username string, password string) {
	boltUrl, ok := os.LookupEnv("ENTITY_STORE")
	if !ok {
		boltUrl = "entity-store:7687"
	}

	username, ok = os.LookupEnv("ENTITY_STORE_USERNAME")
	if !ok {
		username = "neo4j"
	}

	password, ok = os.LookupEnv("ENTITY_STORE_PASSWORD")
	if !ok {
		password = "qwerqwer"
	}

	return
}
