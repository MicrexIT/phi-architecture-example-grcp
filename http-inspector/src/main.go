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
	Title    string   `json:"title,omitempty"`
	Tagline  string   `json:"tagline,omitempty"`
	Cast     []Person `json:"cast,omitempty"`
}

// Person is a person in a movie
type Person struct {
	Job  string   `json:"job"`
	Role []string `json:"role"`
	Name string   `json:"name"`
}

// D3Response is the graph response
type D3Response struct {
	Nodes []Node `json:"nodes"`
	Links []Link `json:"links"`
}

// Node is the graph response node
type Node struct {
	Title string `json:"title"`
	Label string `json:"label"`
}

// Link is the graph response link
type Link struct {
	Source int `json:"source"`
	Target int `json:"target"`
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
		query := `
			MATCH (p:Product)<-[:BOUGHT|:WATCHED]-(c:Person)
			RETURN p.name as product, collect(c.name) as customers`

		url, username, password := environmentVariables()
		client := neo4j.NewClient(
			url,
			username,
			password,
		)

		// customer := Customer{}
		product := Product{}
		customers := []string{}

		d3Resp := D3Response{}
		job := func(record neo4j.Record) error {
			// customer.Name = record["name"].(string)
			// customer.Name = record["name"].(string)
			// customer.Name = record["name"].(string)
			product.Name = record["product"].(string)
			customers = interfaceSliceToString(record["customers"].([]interface{}))

			d3Resp.Nodes = append(d3Resp.Nodes, Node{Title: product.Name, Label: "product"})

			productIndex := len(d3Resp.Nodes) - 1

			// TODO: REFACTOR
			for _, customer := range customers {
				customerIndex := -1
				for i, node := range d3Resp.Nodes {
					if customer == node.Title && node.Label == "customer" {
						customerIndex = i
						break
					}
				}
				if customerIndex == -1 {
					d3Resp.Nodes = append(d3Resp.Nodes, Node{Title: customer, Label: "customer"})
					d3Resp.Links = append(d3Resp.Links, Link{Source: len(d3Resp.Nodes) - 1, Target: productIndex})
				} else {
					d3Resp.Links = append(d3Resp.Links, Link{Source: customerIndex, Target: productIndex})

				}
			}
			fmt.Println("sending d3resp:")
			fmt.Println(d3Resp)

			return nil

		}

		err := client.Read(query, job)

		c.JSON(200, d3Resp)
		if err != nil && err != io.EOF {
			log.Println("error querying graph:", err)
			return
		} else if len(d3Resp.Nodes) == 0 {
			fmt.Println("no nodes")
			return
		}

	})
	r.Run(":8080")

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
