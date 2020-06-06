package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/MicrexIT/neo4j-driver-client"
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

var (
	neo4jURL = "entity-store:7687"
)

func interfaceSliceToString(s []interface{}) []string {
	o := make([]string, len(s))
	for idx, item := range s {
		o[idx] = item.(string)
	}
	return o
}

func defaultHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	body, _ := ioutil.ReadFile("public/index.html")
	w.Write(body)
}

func searchHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	productName := strings.ToLower(strings.TrimSpace(req.URL.Query()["q"][0]))

	query := fmt.Sprintf(
		`MATCH
		(product:Product)
	WHERE
		product.name == %s
	RETURN
		product.name as name, product.bought as bought, product.watched as watched`, productName)

    url, username, password := environmentVariables()
	client := neo4j.NewClient(
		url,
		username,
		password,
	)

	// customer := Customer{}
	products := []Product{}
	product := Product{}

	fmt.Println(product)
	job := func(record neo4j.Record) error {
		// customer.Name = record["name"].(string)
		// customer.Name = record["name"].(string)
		// customer.Name = record["name"].(string)
		product.Name = record["name"].(string)
		product.Bought = record["bought"].(int64)
		product.Watched = record["watched"].(int64)
		products = append(products, product)
		return nil
	}

	err := client.Read(query, job)

	fmt.Println(product)

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(product)
	if err != nil {
		log.Println("error writing search response:", err)
		w.WriteHeader(500)
		w.Write([]byte("An error occurred writing response"))
	}
}

func graphHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	query := `
	MATCH
		(p:Product)<-[:BOUGHT|:WATCHED]-(c:Customer)
	RETURN
		p.name as product, collect(c.name) as customers
`

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

		return nil
	}

	err := client.Read(query, job)

	if err != nil && err != io.EOF {
		log.Println("error querying graph:", err)
		w.WriteHeader(500)
		w.Write([]byte("An error occurred querying the DB"))
		return
	} else if len(d3Resp.Nodes) == 0 {
		w.WriteHeader(404)
		return
	}

	err = json.NewEncoder(w).Encode(d3Resp)
	if err != nil {
		log.Println("error writing graph response:", err)
		w.WriteHeader(500)
		w.Write([]byte("An error occurred writing response"))
	}
}

func init() {
	if os.Getenv("ENTITY_STORE") != "" {
		neo4jURL = os.Getenv("ENTITY_STORE")
	}
}

func main() {
	serveMux := http.NewServeMux()
	serveMux.HandleFunc("/", defaultHandler)
	serveMux.HandleFunc("/search", searchHandler)
	serveMux.HandleFunc("/graph", graphHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Starting server on port %s with neo4j %s", port, neo4jURL)
	panic(http.ListenAndServe(":"+port, serveMux))
}


func environmentVariables() ( boltUrl string, username string, password string) {
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
