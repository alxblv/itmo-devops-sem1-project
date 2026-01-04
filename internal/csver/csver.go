package csver

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"internal/common"
)

func ParseCsvToSliceOfStructs(csvBytes []byte) ([]common.Info, error) {
	var records []common.Info

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
			if len(record) != len(common.KnownFields) {
				errStr := fmt.Sprintf("amount of columns in csv %d does not match amount of necessary fields %d",
					len(record), len(common.KnownFields))
				log.Println(errStr)
				return nil, errors.New(errStr)
			}

			for indx, fieldName := range record {
				fmt.Printf("Read fieldName %s\n", fieldName)
				_, found := common.KnownFields[fieldName]
				if !found {
					errStr := fmt.Sprintf("unexpected field name %s in the heading record of csv", fieldName)
					log.Println(errStr)
					return nil, errors.New(errStr)
				}

				IndexToKnownFields[indx] = fieldName
			}

		} else {

			var currentInfo common.Info
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

func BuildCsvFile(records []common.Info) (*os.File, error) {

	tempFilePath := filepath.Join(common.TempPath, common.DataFileName)
	file, err := os.Create(tempFilePath)
	if err != nil {
		errStr := fmt.Sprintf("error in Create(): %v", err)
		log.Println(errStr)
		return nil, errors.New(errStr)
	}

	csvWriter := csv.NewWriter(file)

	// first write heading line
	var headingLine []string

	for columnName := range common.KnownFields {
		headingLine = append(headingLine, columnName)
	}

	err = csvWriter.Write(headingLine)
	if err != nil {
		errStr := fmt.Sprintf("csvWriter failed to Write() heading line: %v", err)
		log.Println(errStr)
		file.Close()
		os.Remove(file.Name())
		return nil, errors.New(errStr)
	}

	csvWriter.Flush()

	for index, record := range records {
		var singleLine []string

		idInStr := strconv.Itoa(record.Id)
		singleLine = append(singleLine, idInStr)

		singleLine = append(singleLine, record.Name)
		singleLine = append(singleLine, record.Category)

		priceInStr := strconv.FormatFloat(record.Price, 'f', 2, 64)
		singleLine = append(singleLine, priceInStr)

		singleLine = append(singleLine, record.CreateDate.Format("2006-01-02"))

		fmt.Printf("Record %d prepared for CSV writer %v\n", index, singleLine)

		err := csvWriter.Write(singleLine)
		if err != nil {
			errStr := fmt.Sprintf("csvWriter failed to Write(): %v", err)
			log.Println(errStr)
			file.Close()
			os.Remove(file.Name())
			return nil, errors.New(errStr)
		}

		csvWriter.Flush()

	}

	fmt.Printf("BuildCsvFile() of %s done: wrote %d records\n", tempFilePath, len(records))
	return file, nil
}
