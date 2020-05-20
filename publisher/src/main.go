package main

import (
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
)

func handler(w http.ResponseWriter, r *http.Request) {
	path := strings.Split(r.URL.Path[1:], "/")
	switch path[1] {
	case "product":
		productHandler(w, r, path)

	case "customer":
		customerHandler(w, r, path)

	default:
		http.NotFound(w, r)
	}
}

type Event struct {
	Name    string      `json:"name"`
	Version string      `json:"version"`
	Payload interface{} `json:"payload"`
}

func productHandler(w http.ResponseWriter, r *http.Request, path []string) {

	fmt.Println("product handler starting...")
	if path[3] != "watched" {
		fmt.Printf("cannot find the %v path in productHandler\n", path[3])
		http.NotFound(w, r)
	}
	w.Header().Set("Content-Type", "application/json")
	// 1. determine method POST/OPTION
	if r.Method == http.MethodPost {
		productName := path[2]
		if !isAlphaNum(productName) {
			io.WriteString(w, productName+" is invalid")
			http.NotFound(w, r)
		}

		// get visitor name from body
		rawBody, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Fatalln(err)
		}

		type Body struct {
			Visitor string `json:"visitor"`
		}
		body := Body{Visitor: "anonymous"}

		err = json.Unmarshal(rawBody, &body)
		if err != nil {
			log.Fatalln(err)
		}

		// write to kafka
		msg := createProductMessage(w, "product_watched", path[0], productName, body.Visitor)
		pushMessage(msg, w, r)

	} else if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
	} else {
		http.NotFound(w, r)
	}
}

func customerHandler(w http.ResponseWriter, r *http.Request, path []string) {
	if path[3] != "bought" {
		io.WriteString(w, path[3]+" is invalid")
		http.NotFound(w, r)
	}

	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodPost {
		customerName := path[2]
		if !isAlpha(customerName) {
			io.WriteString(w, customerName+" is invalid")
			http.NotFound(w, r)
		}

		// get body products
		// for each product
		// get product from body
		rawBody, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Fatalln(err)
		}

		type Body struct {
			Products []string `json:"products"`
		}

		body := Body{Products: []string{}}

		err = json.Unmarshal(rawBody, &body)
		if err != nil {
			log.Fatalln(err)
		}

		for _, productName := range body.Products {
			// TODO: Probably could use goroutines here
			// Can it impact my kafka writer?
			msg := createCustomerMessage(w, "product_bought", path[0], productName, customerName)
			pushMessage(msg, w, r)
		}

	} else if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
	} else {
		http.NotFound(w, r)
		return
	}
}
func createCustomerMessage(w http.ResponseWriter, msgName string, msgVersion string, productName string, customerName string) *kafka.Message {
	type Payload struct {
		Product  string `json:"product"`
		Customer string `json:"customer"`
	}

	event := Event{
		Name:    msgName,
		Version: msgVersion,
		Payload: Payload{
			Product:  productName,
			Customer: customerName,
		},
	}
	return createMessage(w, &event)
}
func createProductMessage(w http.ResponseWriter, msgName string, msgVersion string, productName string, customerName string) *kafka.Message {
	type Payload struct {
		Product string `json:"product"`
		Visitor string `json:"visitor"`
	}

	event := Event{
		Name:    msgName,
		Version: msgVersion,
		Payload: Payload{
			Product: productName,
			Visitor: customerName,
		},
	}
	return createMessage(w, &event)
}

func createMessage(w http.ResponseWriter, event *Event) *kafka.Message {
	value, err := json.Marshal(*event)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		log.Fatalln(err)
	}

	msg := kafka.Message{
		Value: value,
	}

	return &msg
}

func pushMessage(msg *kafka.Message, w http.ResponseWriter, r *http.Request) {
	kafkaWriter := getKafkaWriter()
	err := kafkaWriter.WriteMessages(r.Context(), *msg)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(err.Error()))
		log.Fatalln(err)
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "%s\n", msg)
}

func getKafkaWriter() *kafka.Writer {
	eventStore, ok := os.LookupEnv("EVENT_STORE")
	if !ok {
		eventStore = "localhost:9092"
	}

	topic, ok := os.LookupEnv("TOPIC")
	if !ok {
		topic = "events"
	}
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{eventStore},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
}

func main() {
	http.ListenAndServe(":80", http.HandlerFunc(handler))
	fmt.Println("server started")
}

// isAlphaNum returns true if the string passed as argument is alphanumeric
func isAlphaNum(s string) bool {
	return regexp.MustCompile(`^[[:alnum:]]+$`).MatchString(s)
}

// isAlphaNum returns true if the string passed as argument is alpha a-zA-Z
func isAlpha(s string) bool {
	return regexp.MustCompile(`^[[:alpha:]]+$`).MatchString(s)
}
