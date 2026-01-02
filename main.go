package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"internal/helper"
)

func pricesHandler(w http.ResponseWriter, r *http.Request) {

	switch r.Method {
	case "GET":
		fmt.Fprintf(w, "GET request was processed")
	case "POST":
		fmt.Fprintf(w, "POST request was processed")

		localFile, err := helper.SaveReceivedFile(r)
		if err != nil {
			log.Printf("error in SaveReceivedFile() %v", err)
			return
		}

		defer localFile.Close()

		csvBytes, err := helper.UnzipAndStoreCSV(localFile)
		if err != nil {
			log.Printf("error in UnzipAndStoreCSV() %v", err)
			return
		}

		fmt.Printf("gonna remove file %s\n", localFile.Name())
		os.Remove(localFile.Name()) // no need to keep file anymore

		fmt.Printf("obtained CSV: %s\n", csvBytes)

		records, err := helper.ParseCsvToSliceOfStructs(csvBytes)
		if err != nil {
			log.Printf("error in ParseCsvToSliceOfStructs() %v", err)
			return
		}

		fmt.Print(records)

		err = helper.InsertToBase(records)
		if err != nil {
			log.Printf("error in InsertToBase() %v", err)
			return
		}

	default:
		http.Error(w, "Unsupported request method", http.StatusMethodNotAllowed)
	}
}

func main() {
	log.SetFlags(log.Llongfile)
	mux := http.NewServeMux()
	mux.HandleFunc(`/api/v0/prices`, pricesHandler)

	err := http.ListenAndServe(`:8080`, mux)
	if err != nil {
		panic(err)
	}
}
