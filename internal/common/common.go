package common

import "time"

type Info struct {
	Id         int
	Name       string
	Category   string
	Price      float64
	CreateDate time.Time
}

const DataFileName string = "data.csv"
const ZipFileName string = "data.zip"
const TempPath string = "/tmp"

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
