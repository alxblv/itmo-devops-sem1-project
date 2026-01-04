package sqler

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"

	_ "github.com/lib/pq"

	"internal/common"
)

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

func InsertToBase(records []common.Info) error {

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
	var stats common.Stats

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

func CollectPricesRecordsFromBase() ([]common.Info, error) {
	var records []common.Info

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Printf("failed to opend db %v", err)
		return nil, err
	}
	defer db.Close()

	sqlSelectPricesFromDb := `
SELECT *
FROM prices`

	rows, err := db.Query(sqlSelectPricesFromDb)
	if err != nil {
		log.Printf("failed while getting prices table from db: %v", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var record common.Info
		err = rows.Scan(&record.Id, &record.Name, &record.Category, &record.Price, &record.CreateDate)
		if err != nil {
			log.Printf("failed scanning results of select-from-prices query: %v", err)
			return nil, err
		}
		fmt.Println(record)
		records = append(records, record)
	}

	return records, nil
}
