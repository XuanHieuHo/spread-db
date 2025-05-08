package main

import (
	"github.com/XuanHieuHo/spread-db/gormix/provider"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	readDB, _ := gorm.Open(postgres.Open(""))
	writeDB, _ := gorm.Open(postgres.Open(""))
	db := provider.NewDBProvider(readDB, writeDB)

	var user interface{}
	err := db.Read.Select([]string{"id", "name"}).Where("age > ?", 20).Find(&user)
	if err != nil {
		panic(err)
	}
}
