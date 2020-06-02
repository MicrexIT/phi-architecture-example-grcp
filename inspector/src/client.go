package main

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/neo4j"
)


func interfaceSliceToString(s []interface{}) []string {
	o := make([]string, len(s))
	for idx, item := range s {
		o[idx] = item.(string)
	}
	return o
}

func Neo4jClient() (string, error) {

	fmt.Println("in neo4j client")
	driver, err := neo4j.NewDriver("bolt://localhost:7687", neo4j.BasicAuth("neo4j", "qwerqwer", ""))
	if err != nil {
		return "err", err // handle error
	}
	// handle driver lifetime based on your application lifetime requirements
	// driver's lifetime is usually bound by the application lifetime, which usually implies one driver instance per application
	defer driver.Close()

	session, err := driver.Session(neo4j.AccessModeWrite)
	if err != nil {
		return "err", err
	}

	defer session.Close()
	result, err := session.Run("MATCH (p:Person) RETURN p",map[string]interface{}{})
	if err != nil {
		return "err", err // handle error
	}
	// result, err := session.Run("MATCH (p:Person)--(pr:Product) RETURN p, pr",map[string]interface{}{})
	// if err != nil {
	// 	return "err", err // handle error
	// }

	for result.Next() {
		// fmt.Printf("CCreated Item with Id = '%d' and Name = '%s' created Item with Id = '%d' and Name = '%s'\n", result.Record().GetByIndex(0).(int64), result.Record().GetByIndex(1).(string))
		fmt.Println("printing results in neo4j")
		for _, _ = range result.Record().Values() {
			fmt.Println(11)
		}
		// fmt.Print ("result.Record(): ", result.Record().Values())

	}
	if err = result.Err(); err != nil {
		return "err", err // handle error
	}

	return "ok", nil
}

