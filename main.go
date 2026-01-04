package main

import (
	"fmt"
	"log"
	"net/http"

	"internal/csver"
	"internal/receiver"
	"internal/responder"
	"internal/sqler"
	"internal/zipper"
)

func pricesHandler(w http.ResponseWriter, r *http.Request) {

	switch r.Method {
	case "GET":
		getHandler(w, r)
	case "POST":
		postHandler(w, r)
	default:
		http.Error(w, "Unsupported request method", http.StatusMethodNotAllowed)
	}
}

func main() {
	log.SetFlags(log.Llongfile)
	sqler.PrepareDbConnectionInfo()
	mux := http.NewServeMux()
	mux.HandleFunc(`/api/v0/prices`, pricesHandler)

	err := http.ListenAndServe(`:8080`, mux)
	if err != nil {
		panic(err)
	}
}

/**********************************************************/
func getHandler(w http.ResponseWriter, r *http.Request) {
	records, err := sqler.CollectPricesRecordsFromBase()
	if err != nil {
		log.Printf("error in CollectPricesRecordsFromBase() %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	dataFile, err := csver.BuildCsvFile(records)
	if err != nil {
		log.Printf("error in BuildCsvFile() %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	fileinfo, _ := dataFile.Stat()
	fmt.Printf("Size of obtained CSV file %s is %d\n", dataFile.Name(), fileinfo.Size())

	archive, err := zipper.ZipBuiltCSV(dataFile)
	if err != nil {
		log.Printf("error in ZipBuiltCSV() %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = responder.SendResponseToGet(w, archive)
	if err != nil {
		log.Printf("error in SendResponseToGet() %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

/**********************************************************/
func postHandler(w http.ResponseWriter, r *http.Request) {
	localFile, err := receiver.SaveReceivedFile(r)
	if err != nil {
		log.Printf("error in SaveReceivedFile() %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	csvBytes, err := zipper.UnzipAndStoreCSV(localFile)
	if err != nil {
		log.Printf("error in UnzipAndStoreCSV() %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	fmt.Printf("obtained CSV: %s\n", csvBytes)

	records, err := csver.ParseCsvToSliceOfStructs(csvBytes)
	if err != nil {
		log.Printf("error in ParseCsvToSliceOfStructs() %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	fmt.Print(records)

	err = sqler.InsertToBase(records)
	if err != nil {
		log.Printf("error in InsertToBase() %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	stats, err := sqler.CollectTotalStatsFromBase()
	if err != nil {
		log.Printf("error in CollectTotalStatsFromBase() %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = responder.SendResponseToPost(w, stats)
	if err != nil {
		log.Printf("error in SendResponseToPost() %v", err)
		return
	}
}
