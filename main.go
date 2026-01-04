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
		localFile, err := helper.SaveReceivedFile(r)
		if err != nil {
			log.Printf("error in SaveReceivedFile() %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		defer localFile.Close()

		csvBytes, err := helper.UnzipAndStoreCSV(localFile)
		if err != nil {
			log.Printf("error in UnzipAndStoreCSV() %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		fmt.Printf("gonna remove file %s\n", localFile.Name())
		os.Remove(localFile.Name()) // no need to keep file anymore

		fmt.Printf("obtained CSV: %s\n", csvBytes)

		records, err := helper.ParseCsvToSliceOfStructs(csvBytes)
		if err != nil {
			log.Printf("error in ParseCsvToSliceOfStructs() %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		fmt.Print(records)

		err = helper.InsertToBase(records)
		if err != nil {
			log.Printf("error in InsertToBase() %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		stats, err := helper.CollectTotalStatsFromBase()
		if err != nil {
			log.Printf("error in CollectTotalStatsFromBase() %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		err = helper.SendResponse(w, stats)
		if err != nil {
			log.Printf("error in SendResponse() %v", err)
			return
		}

	default:
		http.Error(w, "Unsupported request method", http.StatusMethodNotAllowed)
	}
}

func main() {
	log.SetFlags(log.Llongfile)
	helper.PrepareDbConnectionInfo()
	mux := http.NewServeMux()
	mux.HandleFunc(`/api/v0/prices`, pricesHandler)

	err := http.ListenAndServe(`:8080`, mux)
	if err != nil {
		panic(err)
	}
}
