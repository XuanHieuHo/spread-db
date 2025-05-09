package writeonly

import (
	"context"
	"database/sql"
	"github.com/XuanHieuHo/spread-db/gormix"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type writeDB struct {
	db *gorm.DB
}

func (w writeDB) WithContext(ctx context.Context) gormix.WriteOnlyDB {
	return &writeDB{w.db.WithContext(ctx)}
}

func (w writeDB) Table(name string) gormix.WriteOnlyDB {
	return &writeDB{w.db.Table(name)}
}

func (w writeDB) Model(value interface{}) gormix.WriteOnlyDB {
	return &writeDB{w.db.Model(value)}
}

func (w writeDB) Select(query interface{}, args ...interface{}) gormix.WriteOnlyDB {
	return &writeDB{w.db.Select(query, args...)}
}

func (w writeDB) Where(query interface{}, args ...interface{}) gormix.WriteOnlyDB {
	return &writeDB{w.db.Where(query, args...)}
}

func (w writeDB) Joins(query string, args ...interface{}) gormix.WriteOnlyDB {
	return &writeDB{w.db.Joins(query, args...)}
}

func (w writeDB) Group(name string) gormix.WriteOnlyDB {
	return &writeDB{w.db.Group(name)}
}

func (w writeDB) Having(query interface{}, args ...interface{}) gormix.WriteOnlyDB {
	return &writeDB{w.db.Having(query, args...)}
}

func (w writeDB) Order(value interface{}) gormix.WriteOnlyDB {
	return &writeDB{w.db.Order(value)}
}

func (w writeDB) Limit(limit int) gormix.WriteOnlyDB {
	return &writeDB{w.db.Limit(limit)}
}

func (w writeDB) Offset(offset int) gormix.WriteOnlyDB {
	return &writeDB{w.db.Offset(offset)}
}

func (w writeDB) Scopes(funcs ...func(db gormix.WriteOnlyDB) gormix.WriteOnlyDB) gormix.WriteOnlyDB {
	var gormScopes []func(db *gorm.DB) *gorm.DB
	for _, fn := range funcs {
		gormScopes = append(gormScopes, func(gormDB *gorm.DB) *gorm.DB {
			result := fn(&writeDB{db: gormDB})

			if resultDB, ok := result.(*writeDB); ok {
				return resultDB.db
			}
			return gormDB
		})
	}
	return &writeDB{w.db.Scopes(gormScopes...)}
}

func (w writeDB) Unscoped() gormix.WriteOnlyDB {
	return &writeDB{w.db.Unscoped()}
}

func (w writeDB) Preload(query string, args ...interface{}) gormix.WriteOnlyDB {
	return &writeDB{w.db.Preload(query, args...)}
}

func (w writeDB) Distinct(args ...interface{}) gormix.WriteOnlyDB {
	return &writeDB{w.db.Distinct(args...)}
}

func (w writeDB) Omit(columns ...string) gormix.WriteOnlyDB {
	return &writeDB{w.db.Omit(columns...)}
}

func (w writeDB) Raw(sql string, values ...interface{}) gormix.WriteOnlyDB {
	return &writeDB{w.db.Raw(sql, values...)}
}

func (w writeDB) Find(dest interface{}, conds ...interface{}) gormix.WriteOnlyDB {
	return &writeDB{db: w.db.Find(dest, conds...)}
}

func (w writeDB) First(dest interface{}, conds ...interface{}) gormix.WriteOnlyDB {
	return &writeDB{db: w.db.First(dest, conds...)}
}

func (w writeDB) Last(dest interface{}, conds ...interface{}) gormix.WriteOnlyDB {
	return &writeDB{db: w.db.Last(dest, conds...)}
}

func (w writeDB) Take(dest interface{}, conds ...interface{}) gormix.WriteOnlyDB {
	return &writeDB{db: w.db.Take(dest, conds...)}
}

func (w writeDB) Scan(dest interface{}) gormix.WriteOnlyDB {
	return &writeDB{db: w.db.Scan(dest)}
}

func (w writeDB) Pluck(column string, dest interface{}) gormix.WriteOnlyDB {
	return &writeDB{db: w.db.Pluck(column, dest)}
}

func (w writeDB) Count(count *int64) gormix.WriteOnlyDB {
	return &writeDB{db: w.db.Count(count)}
}

func (w writeDB) Row() *sql.Row {
	return w.db.Row()
}

func (w writeDB) Rows() (*sql.Rows, error) {
	return w.db.Rows()
}

func (w writeDB) Debug() gormix.WriteOnlyDB {
	return &writeDB{w.db.Debug()}
}

func (w writeDB) Statement() *gorm.Statement {
	return w.db.Statement
}

func (w writeDB) Error() error {
	return w.db.Error
}

func (r writeDB) Dialector() gorm.Dialector {
	return r.db.Dialector
}

func (w writeDB) Session(session *gorm.Session) gormix.WriteOnlyDB {
	return &writeDB{db: w.db.Session(session)}
}

func (w writeDB) Create(value interface{}) gormix.WriteOnlyDB {
	return &writeDB{w.db.Create(value)}
}

func (w writeDB) CreateInBatches(value interface{}, batchSize int) gormix.WriteOnlyDB {
	return &writeDB{w.db.CreateInBatches(value, batchSize)}
}

func (w writeDB) Save(value interface{}) gormix.WriteOnlyDB {
	return &writeDB{w.db.Save(value)}
}

func (w writeDB) Update(column string, value interface{}) gormix.WriteOnlyDB {
	return &writeDB{w.db.Update(column, value)}
}

func (w writeDB) Updates(values interface{}) gormix.WriteOnlyDB {
	return &writeDB{w.db.Updates(values)}
}

func (w writeDB) UpdateColumn(column string, value interface{}) gormix.WriteOnlyDB {
	return &writeDB{w.db.UpdateColumn(column, value)}
}

func (w writeDB) UpdateColumns(values interface{}) gormix.WriteOnlyDB {
	return &writeDB{w.db.UpdateColumns(values)}
}

func (w writeDB) Delete(value interface{}, conds ...interface{}) gormix.WriteOnlyDB {
	return &writeDB{w.db.Delete(value, conds...)}
}

func (w writeDB) Exec(sql string, values ...interface{}) gormix.WriteOnlyDB {
	return &writeDB{w.db.Exec(sql, values...)}
}

func (w writeDB) Transaction(fc func(tx gormix.WriteOnlyDB) error, opts ...*sql.TxOptions) error {
	return w.db.Transaction(func(tx *gorm.DB) error {
		return fc(&writeDB{db: tx})
	}, opts...)
}

func (w writeDB) Begin(opts ...*sql.TxOptions) gormix.WriteOnlyDB {
	return &writeDB{w.db.Begin(opts...)}
}

func (w writeDB) Commit() error {
	return w.db.Commit().Error
}

func (w writeDB) Rollback() error {
	return w.db.Rollback().Error
}

func (w writeDB) Association(column string) *gorm.Association {
	return w.db.Association(column)
}

func (w writeDB) Clauses(conds ...clause.Expression) gormix.WriteOnlyDB {
	return &writeDB{w.db.Clauses(conds...)}
}

func New(db *gorm.DB) gormix.WriteOnlyDB {
	return &writeDB{db: db}
}
