package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"github.com/segmentio/kafka-go"
	"net/http"
	"os"
	"regexp"
	"strings"
)

type Event struct {
	Name    string      `json:"name"`
	Version string      `json:"version"`
	Payload interface{} `json:"payload"`
}

func main() {
	http.ListenAndServe(":80", http.HandlerFunc(handler))
	fmt.Println("server started")
}

func handler(w http.ResponseWriter, r *http.Request) {
	path := strings.Split(r.URL.Path[1:], "/") // refactor
	switch path[1] {
	case "product":
		productHandler(w, r, path)

	case "customer":
		customerHandler(w, r, path)

	default:
		http.NotFound(w, r)
	}
}

func productHandler(w http.ResponseWriter, r *http.Request, path []string) {
	if path[3] != "watched" {
		fmt.Printf("cannot find the %v path in productHandler\n", path[3])
		http.NotFound(w, r)
		return
	}

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}

	productName := path[2]
	if !isAlphaNum(productName) {
		io.WriteString(w, productName+" is invalid")
		http.NotFound(w, r)
		return
	}

	rawBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(err.Error()))
		return
	}

	body := struct {
		Visitor string `json:"visitor"`
	}{
		Visitor: "anonymous",
	}

	err = json.Unmarshal(rawBody, &body)
	if err != nil {
		log.Fatalln(err)
	}

	w.Header().Set("Content-Type", "application/json")
	event := Event{
		Name:    "",
		Version: "v1.0",
		Payload: struct {
			Product string `json:"product"`
			Visitor string `json:"visitor"`
		}{
			Product: productName,
			Visitor: body.Visitor,
		},
	}
	msg, err := createMessage(&event)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		fmt.Println(err)
	}
	status, err := pushMessage(msg)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		fmt.Println(err)
	}
	w.WriteHeader(int(status))
	fmt.Fprintf(w, "%s\n", event)

}

func customerHandler(w http.ResponseWriter, r *http.Request, path []string) {
	if path[3] != "bought" {
		io.WriteString(w, path[3]+" is invalid")
		http.NotFound(w, r)
		return
	}
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}

	customerName := path[2]
	if !isAlpha(customerName) {
		io.WriteString(w, customerName+" is invalid")
		http.NotFound(w, r)
		return
	}

	rawBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(err.Error()))
		return
	}

	body :=  struct {
		Products []string `json:"products"`
	}{Products: []string{}}

	err = json.Unmarshal(rawBody, &body)
	if err != nil {
		log.Fatalln(err)
	}

	w.Header().Set("Content-Type", "application/json")
	for _, productName := range body.Products {
		event := Event{
			Name:    "",
			Version: "v1.0",
			Payload: struct {
				Product string `json:"product"`
				Customer string `json:"customer"`
			}{
				Product: productName,
				Customer: customerName,
			},
		}
		msg, err := createMessage(&event)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			fmt.Println(err)
		}
		status, err := pushMessage(msg)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			fmt.Println(err)
		}
		w.WriteHeader(int(status))
		fmt.Fprintf(w, "%s\n", event)
	}
}


func createMessage(event *Event) (*kafka.Message, error) {
	value, err := json.Marshal(*event)
	if err != nil {
		return nil, err
	}
	msg := kafka.Message{
		Value: value,
	}
	return &msg, nil
}

func pushMessage(msg *kafka.Message) (http.ConnState , error) {
	kafkaWriter := getKafkaWriter()
	err := kafkaWriter.WriteMessages(context.Background(), *msg)

	if err != nil {
		return http.StatusBadRequest, err
	}
	return http.StatusOK, err
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

// isAlphaNum returns true if the string passed as argument is alphanumeric
func isAlphaNum(s string) bool {
	return regexp.MustCompile(`^[[:alnum:]]+$`).MatchString(s)
}

// isAlphaNum returns true if the string passed as argument is alpha a-zA-Z
func isAlpha(s string) bool {
	return regexp.MustCompile(`^[[:alpha:]]+$`).MatchString(s)
}
