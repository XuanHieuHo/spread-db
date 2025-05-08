package readonly

import (
	"context"
	"database/sql"
	"github.com/XuanHieuHo/spread-db/gormix"
	"gorm.io/gorm"
)

type readDB struct {
	db *gorm.DB
}

func (r readDB) WithContext(ctx context.Context) gormix.ReadOnlyDB {
	return &readDB{db: r.db.WithContext(ctx)}
}

func (r readDB) Table(name string) gormix.ReadOnlyDB {
	return &readDB{db: r.db.Table(name)}
}

func (r readDB) Model(value interface{}) gormix.ReadOnlyDB {
	return &readDB{db: r.db.Model(value)}
}

func (r readDB) Select(query interface{}, args ...interface{}) gormix.ReadOnlyDB {
	return &readDB{db: r.db.Select(query, args...)}
}

func (r readDB) Where(query interface{}, args ...interface{}) gormix.ReadOnlyDB {
	return &readDB{db: r.db.Where(query, args...)}
}

func (r readDB) Joins(query string, args ...interface{}) gormix.ReadOnlyDB {
	return &readDB{db: r.db.Joins(query, args...)}
}

func (r readDB) Group(name string) gormix.ReadOnlyDB {
	return &readDB{db: r.db.Group(name)}
}

func (r readDB) Having(query interface{}, args ...interface{}) gormix.ReadOnlyDB {
	return &readDB{db: r.db.Having(query, args...)}
}

func (r readDB) Order(value interface{}) gormix.ReadOnlyDB {
	return &readDB{db: r.db.Order(value)}
}

func (r readDB) Limit(limit int) gormix.ReadOnlyDB {
	return &readDB{db: r.db.Limit(limit)}
}

func (r readDB) Offset(offset int) gormix.ReadOnlyDB {
	return &readDB{db: r.db.Offset(offset)}
}

func (r readDB) Scopes(funcs ...func(db gormix.ReadOnlyDB) gormix.ReadOnlyDB) gormix.ReadOnlyDB {
	var gormScopes []func(db *gorm.DB) *gorm.DB
	for _, fn := range funcs {
		gormScopes = append(gormScopes, func(gormDB *gorm.DB) *gorm.DB {
			result := fn(&readDB{db: gormDB})

			if resultDB, ok := result.(readDB); ok {
				return resultDB.db
			}
			return gormDB
		})
	}
	return &readDB{db: r.db.Scopes(gormScopes...)}
}

func (r readDB) Unscoped() gormix.ReadOnlyDB {
	return &readDB{db: r.db.Unscoped()}
}

func (r readDB) Preload(query string, args ...interface{}) gormix.ReadOnlyDB {
	return &readDB{db: r.db.Preload(query, args...)}
}

func (r readDB) Distinct(args ...interface{}) gormix.ReadOnlyDB {
	return &readDB{db: r.db.Distinct(args...)}
}

func (r readDB) Omit(columns ...string) gormix.ReadOnlyDB {
	return &readDB{db: r.db.Omit(columns...)}
}

func (r readDB) Raw(sql string, values ...interface{}) gormix.ReadOnlyDB {
	return &readDB{db: r.db.Raw(sql, values...)}
}

func (r readDB) Find(dest interface{}, conds ...interface{}) gormix.ReadOnlyDB {
	return &readDB{db: r.db.Find(dest, conds...)}
}

func (r readDB) First(dest interface{}, conds ...interface{}) gormix.ReadOnlyDB {
	return &readDB{db: r.db.First(dest, conds...)}
}

func (r readDB) Last(dest interface{}, conds ...interface{}) gormix.ReadOnlyDB {
	return &readDB{db: r.db.Last(dest, conds...)}
}

func (r readDB) Take(dest interface{}, conds ...interface{}) gormix.ReadOnlyDB {
	return &readDB{db: r.db.Take(dest, conds...)}
}

func (r readDB) Scan(dest interface{}) gormix.ReadOnlyDB {
	return &readDB{db: r.db.Scan(dest)}
}

func (r readDB) Pluck(column string, dest interface{}) gormix.ReadOnlyDB {
	return &readDB{db: r.db.Pluck(column, dest)}
}

func (r readDB) Count(count *int64) gormix.ReadOnlyDB {
	return &readDB{db: r.db.Count(count)}
}

func (r readDB) Row() *sql.Row {
	return r.db.Row()
}

func (r readDB) Rows() (*sql.Rows, error) {
	return r.db.Rows()
}

func (r readDB) Debug() gormix.ReadOnlyDB {
	return &readDB{db: r.db.Debug()}
}

func (r readDB) Statement() *gorm.Statement {
	return r.db.Statement
}

func (r readDB) Error() error {
	return r.db.Error
}

func (r readDB) Dialector() gorm.Dialector {
	return r.db.Dialector
}

func (r readDB) Session(session *gorm.Session) gormix.ReadOnlyDB {
	return &readDB{db: r.db.Session(session)}
}

func New(db *gorm.DB) gormix.ReadOnlyDB {
	return &readDB{db: db}
}
