package main

import (
	"fmt"
	"log"
	"net/http"

	"internal/helper"
)

func pricesHandler(w http.ResponseWriter, r *http.Request) {

	switch r.Method {
	case "GET":

		records, err := helper.CollectPricesRecordsFromBase()
		if err != nil {
			log.Printf("error in CollectPricesRecordsFromBase() %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		dataFile, err := helper.BuildCsvFile(records)
		if err != nil {
			log.Printf("error in BuildCsvFile() %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		fileinfo, _ := dataFile.Stat()
		fmt.Printf("Size of obtained CSV file %s is %d\n", dataFile.Name(), fileinfo.Size())

		err = helper.ZipBuiltCSV(dataFile)
		if err != nil {
			log.Printf("error in ZipBuiltCSV() %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

	case "POST":
		localFile, err := helper.SaveReceivedFile(r)
		if err != nil {
			log.Printf("error in SaveReceivedFile() %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		csvBytes, err := helper.UnzipAndStoreCSV(localFile)
		if err != nil {
			log.Printf("error in UnzipAndStoreCSV() %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

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

		err = helper.SendResponseToPost(w, stats)
		if err != nil {
			log.Printf("error in SendResponseToPost() %v", err)
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
