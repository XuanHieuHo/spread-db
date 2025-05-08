package provider

import (
	"github.com/XuanHieuHo/spread-db/gormix"
	"github.com/XuanHieuHo/spread-db/gormix/readonly"
	"github.com/XuanHieuHo/spread-db/gormix/writeonly"
	"gorm.io/gorm"
)

type DBProvider struct {
	Read  gormix.ReadOnlyDB
	Write gormix.WriteOnlyDB
}

func NewDBProvider(readDB *gorm.DB, writeDB *gorm.DB) *DBProvider {
	return &DBProvider{
		Read:  readonly.New(readDB),
		Write: writeonly.New(writeDB),
	}
}
