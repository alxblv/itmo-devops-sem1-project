package helper

import (
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type Info struct {
	Id         int
	Name       string
	Category   string
	Price      float64
	CreateDate time.Time
}

var KnownFields = map[string]struct{}{
	"id":          struct{}{},
	"name":        struct{}{},
	"category":    struct{}{},
	"price":       struct{}{},
	"create_date": struct{}{},
}

type Stats struct {
	TotalItems      int `json:"total_items"`
	TotalCategories int `json:"total_categories"`
	TotalPrice      int `json:"total_price"`
}

// TODO: read from config? command-line args?
const (
	host     = "localhost"
	port     = 5432
	user     = "validator"
	password = "val1dat0r"
	dbname   = "project-sem-1"
)

var psqlInfo string

func PrepareDbConnectionInfo() {
	psqlInfo = fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
}

func SaveReceivedFile(r *http.Request) (*os.File, error) {
	contentTypeStuff := strings.Split(r.Header.Get("Content-Type"), ";")
	fmt.Printf("Content-Type in request %v\n", contentTypeStuff[0])

	if contentTypeStuff[0] != "multipart/form-data" {
		errStr := fmt.Sprintf("no idea how to handle %v further", contentTypeStuff[0])
		log.Println(errStr)
		return nil, errors.New(errStr)
	}

	multipartFile, header, err := r.FormFile("file")

	if err != nil {
		errStr := fmt.Sprintf("error while trying to read file from POST request %v", err)
		log.Println(errStr)
		return nil, errors.New(errStr)
	}

	fmt.Printf("Content-Length from request is %d, header.Size %d\n", r.ContentLength, header.Size)

	defer multipartFile.Close()

	tempFilePath := filepath.Join("/tmp", header.Filename)
	localFile, err := os.Create(tempFilePath)
	if err != nil {
		errStr := fmt.Sprintf("error while creating %s locally %v", header.Filename, err)
		log.Println(errStr)
		return nil, errors.New(errStr)
	}

	io.Copy(localFile, multipartFile)

	return localFile, nil
}

func UnzipAndStoreCSV(localFile *os.File) ([]byte, error) {

	var unzipped []byte
	zipReader, err := zip.OpenReader(localFile.Name())

	fmt.Printf("localfile: %s\n", localFile.Name())
	if err != nil {
		errStr := fmt.Sprintf("error in zip.OpenReader() %v", err)
		log.Println(errStr)
		return nil, errors.New(errStr)
	}
	defer zipReader.Close()

	wdPath, err := os.Getwd()
	if err != nil {
		errStr := fmt.Sprintf("failed to get working directory path: %v", err)
		log.Println(errStr)
		return nil, errors.New(errStr)
	}

	fmt.Printf("Working Directory: %s\n", wdPath)

	for _, f := range zipReader.File {
		filename := filepath.Base(f.Name)
		fmt.Printf("filename: %s\n", filename)

		if filename == "data.csv" {

			readCloser, err := f.Open()
			if err != nil {
				errStr := fmt.Sprintf("error in Open(): %v", err)
				log.Println(errStr)
				return nil, errors.New(errStr)
			}

			unzipped = make([]byte, f.FileInfo().Size())
			actuallyReadBytes, err := readCloser.Read(unzipped)
			if err != nil && err != io.EOF {
				errStr := fmt.Sprintf("error in readCloser.Read(): %v", err)
				log.Println(errStr)
				return nil, errors.New(errStr)
			}

			fmt.Printf("Read %d bytes from %s\n", actuallyReadBytes, filename)

			readCloser.Close()

			// fmt.Printf("Unzipped: %s\n", unzipped)
			break // no need to read further
		}
	}

	if len(unzipped) == 0 {
		return nil, errors.New("no data.csv in provided archive")
	}

	return unzipped, nil
}

func ParseCsvToSliceOfStructs(csvBytes []byte) ([]Info, error) {
	var records []Info

	byteReader := bytes.NewReader(csvBytes)
	csvReader := csv.NewReader(byteReader)

	lastRecord := false
	recordNumber := 0
	IndexToKnownFields := make(map[int]string)

	for lastRecord == false {
		record, err := csvReader.Read()
		if err == io.EOF {
			lastRecord = true
			break
		}

		fmt.Printf("Current record number %d\n", recordNumber)

		if recordNumber == 0 {
			// this is a header with column names
			if len(record) != len(KnownFields) {
				errStr := fmt.Sprintf("amount of columns in csv %d does not match amount of necessary fields %d", len(record), len(KnownFields))
				log.Println(errStr)
				return nil, errors.New(errStr)
			}

			for indx, fieldName := range record {
				fmt.Printf("Read fieldName %s\n", fieldName)
				_, found := KnownFields[fieldName]
				if !found {
					errStr := fmt.Sprintf("unexpected field name %s in the heading record of csv", fieldName)
					log.Println(errStr)
					return nil, errors.New(errStr)
				}

				IndexToKnownFields[indx] = fieldName
			}

		} else {

			var currentInfo Info
			for i, value := range record {
				switch IndexToKnownFields[i] {
				case "id":
					currentInfo.Id, err = strconv.Atoi(value)
					if err != nil {
						// errStr := fmt.Sprintf("failed parsing id %v", err)
						// return nil, errors.New(errStr)
						log.Printf("failed to parse id: %v", err)
						continue
					}
				case "name":
					currentInfo.Name = value
				case "category":
					currentInfo.Category = value

				case "price":
					currentInfo.Price, err = strconv.ParseFloat(value, 64)
					if err != nil {
						//errStr := fmt.Sprintf("failed parsing price %v", err)
						//return nil, errors.New(errStr)
						log.Printf("failed to parse price for item with id %d: %v", currentInfo.Id, err)
						continue
					}
				case "create_date":
					currentInfo.CreateDate, err = time.Parse("2006-01-02", value)
					if err != nil {
						// errStr := fmt.Sprintf("failed parsing date %v", err)
						// return nil, errors.New(errStr)
						log.Printf("failed to parse create_date for item with id %d: %v", currentInfo.Id, err)
						continue
					}
				}
			}

			fmt.Printf("Current info %v\n", currentInfo)

			records = append(records, currentInfo)
		}

		recordNumber++
	}

	return records, nil
}

func InsertToBase(records []Info) error {

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Printf("failed to opend db %v", err)
		return err
	}
	defer db.Close()

	sqlStatement := `
INSERT INTO prices (id, name, category, price, create_date)
VALUES ($1, $2, $3, $4, $5)`

	for _, record := range records {

		result, err := db.Exec(sqlStatement, record.Id, record.Name, record.Category, record.Price, record.CreateDate)
		if err != nil {
			log.Printf("failed to execute a query %v", err)
			return err
		}

		fmt.Println("Result of insert:", result)
	}

	return nil
}

func CollectTotalStatsFromBase() ([]byte, error) {
	var jsStats []byte
	var stats Stats

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Printf("failed to opend db %v", err)
		return nil, err
	}
	defer db.Close()

	sqlTotalItemsInDb := `
SELECT COUNT(id)
FROM prices`

	err = db.QueryRow(sqlTotalItemsInDb).Scan(&stats.TotalItems)

	if err != nil {
		log.Printf("failed while quering for total items: %v", err)
		return nil, err
	}

	sqlTotalCategoriesInDb := `
SELECT COUNT(DISTINCT category)
FROM prices`

	err = db.QueryRow(sqlTotalCategoriesInDb).Scan(&stats.TotalCategories)

	if err != nil {
		log.Printf("failed while quering for total categories: %v", err)
		return nil, err
	}

	sqlTotalPriceInDb := `
SELECT SUM(price)
FROM prices`

	var totalPriceFloat float64
	err = db.QueryRow(sqlTotalPriceInDb).Scan(&totalPriceFloat)

	if err != nil {
		log.Printf("failed while quering for total price: %v", err)
		return nil, err
	}

	// do they expect us to round it or simply drop fractional part?
	stats.TotalPrice = int(math.Round(totalPriceFloat))

	jsStats, err = json.Marshal(stats)
	if err != nil {
		log.Printf("error while marshalling json %v", err)
		return nil, err
	}

	// fmt.Printf("Current jsStats %s\n", jsStats)

	return jsStats, nil
}

func SendResponse(w http.ResponseWriter, stats []byte) error {

	w.Header().Set("Content-Type", "application/json")
	_, err := w.Write(stats)
	return err
}
