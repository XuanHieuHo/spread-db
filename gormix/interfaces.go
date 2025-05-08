package gormix

import (
	"context"
	"database/sql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ReadOnlyDB interface {
	WithContext(ctx context.Context) ReadOnlyDB
	Table(name string) ReadOnlyDB
	Model(value interface{}) ReadOnlyDB
	Select(query interface{}, args ...interface{}) ReadOnlyDB
	Where(query interface{}, args ...interface{}) ReadOnlyDB
	Joins(query string, args ...interface{}) ReadOnlyDB
	Group(name string) ReadOnlyDB
	Having(query interface{}, args ...interface{}) ReadOnlyDB
	Order(value interface{}) ReadOnlyDB
	Limit(limit int) ReadOnlyDB
	Offset(offset int) ReadOnlyDB
	Scopes(funcs ...func(db ReadOnlyDB) ReadOnlyDB) ReadOnlyDB
	Unscoped() ReadOnlyDB
	Preload(query string, args ...interface{}) ReadOnlyDB
	Distinct(args ...interface{}) ReadOnlyDB
	Omit(columns ...string) ReadOnlyDB
	Raw(sql string, values ...interface{}) ReadOnlyDB

	// Read Operations
	Find(dest interface{}, conds ...interface{}) ReadOnlyDB
	First(dest interface{}, conds ...interface{}) ReadOnlyDB
	Last(dest interface{}, conds ...interface{}) ReadOnlyDB
	Take(dest interface{}, conds ...interface{}) ReadOnlyDB
	Scan(dest interface{}) ReadOnlyDB
	Pluck(column string, dest interface{}) ReadOnlyDB
	Count(count *int64) ReadOnlyDB
	Row() *sql.Row
	Rows() (*sql.Rows, error)

	// Debug
	Debug() ReadOnlyDB

	Statement() *gorm.Statement
	Error() error
	Dialector() gorm.Dialector
	Session(session *gorm.Session) ReadOnlyDB
}

// WriteOnlyDB
type WriteOnlyDB interface {
	ReadOnlyDB

	// Write Operations
	Create(value interface{}) WriteOnlyDB
	CreateInBatches(value interface{}, batchSize int) WriteOnlyDB
	Save(value interface{}) WriteOnlyDB
	Update(column string, value interface{}) WriteOnlyDB
	Updates(values interface{}) WriteOnlyDB
	UpdateColumn(column string, value interface{}) WriteOnlyDB
	UpdateColumns(values interface{}) WriteOnlyDB
	Delete(value interface{}, conds ...interface{}) WriteOnlyDB
	Exec(sql string, values ...interface{}) WriteOnlyDB

	// Transaction
	Transaction(fc func(tx WriteOnlyDB) error, opts ...*sql.TxOptions) error
	Begin(opts ...*sql.TxOptions) WriteOnlyDB
	Commit() error
	Rollback() error

	// Association methods
	Association(column string) *gorm.Association
	Clauses(conds ...clause.Expression) WriteOnlyDB
}
