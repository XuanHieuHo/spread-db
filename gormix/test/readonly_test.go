package test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XuanHieuHo/spread-db/gormix"
	"github.com/XuanHieuHo/spread-db/gormix/provider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"regexp"
	"strings"
	"testing"
)

type UserDummy struct {
	ID     int64   `gorm:"column:id;type:int64;primaryKey" json:"id"`
	Name   string  `gorm:"column:name;type:varchar(255);not null" json:"name"`
	Email  string  `gorm:"column:email;type:varchar(255);not null" json:"email"`
	Orders []Order `gorm:"foreignKey:UserID"`
}

type City struct {
	ID        int64          `gorm:"column:id;type:int64;primaryKey" json:"id"`
	Name      string         `gorm:"column:name;type:varchar(255);not null" json:"name"`
	DeletedAt gorm.DeletedAt `gorm:"column:deleted_at;type:datetime" json:"deleted_at"`
}

type Order struct {
	ID     int64   `gorm:"column:id;type:int64;primaryKey" json:"id"`
	UserID int64   `gorm:"column:user_id;type:int64;not null" json:"user_id"`
	Price  float64 `gorm:"column:price;type:decimal(10,2);not null" json:"price"`
}

func createDummyUsers(quantity int) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"id", "name", "email"})
	for i := 0; i < quantity; i++ {
		rows.AddRow(i+1, fmt.Sprintf("User %d", i+1), fmt.Sprintf("Email%d@example.com", i+1))
	}
	return rows
}

func setupTestReadDB(t *testing.T) (gormix.ReadOnlyDB, sqlmock.Sqlmock, func()) {
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)

	dialector := postgres.New(postgres.Config{
		Conn:       sqlDB,
		DriverName: "postgres",
	})

	db, err := gorm.Open(dialector, &gorm.Config{})
	require.NoError(t, err)
	cleanup := func() {
		sqlDB.Close()
	}
	dbProvider := provider.NewDBProvider(db, &gorm.DB{})

	return dbProvider.Read, mock, cleanup
}

func TestReadDB_WithContext(t *testing.T) {
	tests := map[string]struct {
		args       []context.Context
		wantResult bool
	}{
		"success": {
			args:       []context.Context{context.Background(), context.Background()},
			wantResult: true,
		},
		"failure": {
			args:       []context.Context{context.Background(), context.WithValue(context.Background(), "id", 1)},
			wantResult: false,
		},
	}

	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			//given
			db, _, _ := setupTestReadDB(t)

			// when
			db1 := db.WithContext(tc.args[0])
			db2 := db.WithContext(tc.args[1])

			// then
			ctx1 := db1.Statement().Context
			ctx2 := db2.Statement().Context
			if tc.wantResult {
				require.Equal(t, ctx1, ctx2)
			} else {
				require.NotEqual(t, ctx1, ctx2)
			}
		})
	}
}

func TestReadDB_Table(t *testing.T) {
	tests := map[string]struct {
		args       interface{}
		wantResult interface{}
	}{
		"one_table_calls": {
			args:       "test_table_success",
			wantResult: "test_table_success",
		},
		"multiple_table_calls": {
			args:       []string{"table1", "table2", "final_table"},
			wantResult: "final_table",
		},
		"empty_table_name": {
			args:       "",
			wantResult: "",
		},
	}

	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			//given
			db, _, _ := setupTestReadDB(t)

			// when
			var dbResult gormix.ReadOnlyDB
			strs, ok := tc.args.([]string)
			if ok {
				for _, str := range strs {
					dbResult = db.Table(str)
				}
			} else {
				dbResult = db.Table(tc.args.(string))
			}

			// then
			require.NoError(t, dbResult.Error())
			require.NotNil(t, dbResult)
			require.Equal(t, tc.wantResult, dbResult.Statement().Table)
		})
	}
}

func TestReadDB_Select(t *testing.T) {
	tests := map[string]struct {
		args       interface{}
		wantResult []string
	}{
		"select string": {
			args:       "id, name",
			wantResult: []string{"id, name"},
		},
		"select with alias": {
			args:       "id as user_id, email",
			wantResult: []string{"id as user_id, email"},
		},
		"select string slice": {
			args:       []string{"id", "email"},
			wantResult: []string{"id", "email"},
		},
		"select with gorm.Expr": {
			args:       gorm.Expr("COUNT(*) as total"),
			wantResult: nil,
		},
		"empty select": {
			wantResult: nil,
		},
	}

	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			// given
			db, _, _ := setupTestReadDB(t)

			// when
			result := db.Select(tc.args)

			// then
			require.NotNil(t, result)
			statement := result.Statement()
			if tc.wantResult == nil {
				require.Empty(t, statement.Selects)
			} else {
				require.Equal(t, tc.wantResult, statement.Selects)
			}
		})
	}
}

func TestReadDB_Where(t *testing.T) {
	tests := map[string]struct {
		query      interface{}
		args       []interface{}
		wantResult string
	}{
		"simple string query": {
			query:      "name = ?",
			args:       []interface{}{"Alice"},
			wantResult: "name = ?",
		},
		"map query": {
			query: map[string]interface{}{"age": 30},
		},
		"struct query": {
			query: struct{ Age int }{Age: 25},
		},
		"gorm.Expr query": {
			query:      gorm.Expr("status = ?", "active"),
			wantResult: "status = ?",
		},
		"invalid type": {
			query: 12345,
			args:  nil,
		},
	}

	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			// given
			db, _, _ := setupTestReadDB(t)

			// when
			result := db.Where(tc.query, tc.args...)

			// then
			require.NoError(t, result.Error())
			statement := result.Statement()

			if _, ok := tc.query.(clause.Expr); ok {
				require.Contains(t, fmt.Sprint(statement.Clauses["WHERE"].Expression), tc.wantResult)
			} else if str, ok := tc.query.(string); ok {
				require.Contains(t, fmt.Sprint(statement.Clauses["WHERE"].Expression), str)
			} else {
				require.NotNil(t, statement.Clauses["WHERE"].Expression)
			}
		})
	}
}

func TestReadDB_Joins(t *testing.T) {
	tests := map[string]struct {
		query   interface{}
		args    []interface{}
		wantSQL interface{}
	}{
		"simple join": {
			query:   "INNER JOIN orders ON users.id = orders.user_id",
			wantSQL: "INNER JOIN orders ON users.id = orders.user_id",
		},
		"join with condition": {
			query:   "LEFT JOIN orders ON users.id = orders.user_id AND orders.status = ?",
			args:    []interface{}{"shipped"},
			wantSQL: "LEFT JOIN orders ON users.id = orders.user_id AND orders.status = ?",
		},
		"multiple joins": {
			query: []string{
				"INNER JOIN orders ON users.id = orders.user_id",
				"LEFT JOIN companies ON users.company_id = companies.id",
			},
			wantSQL: []string{
				"INNER JOIN orders ON users.id = orders.user_id",
				"LEFT JOIN companies ON users.company_id = companies.id",
			},
		},
	}

	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			// given
			db, _, _ := setupTestReadDB(t)

			// when
			result := db
			if joins, ok := tc.query.([]string); ok {
				for _, join := range joins {
					result = result.Joins(join, tc.args...)
				}
			} else {
				result = result.Joins(tc.query.(string), tc.args...)
			}

			// then
			require.NoError(t, result.Error())
			statement := result.Statement()
			require.NotNil(t, statement)
			if want, ok := tc.wantSQL.([]string); ok {
				for i, join := range want {
					require.Equal(t, statement.Joins[i].Name, join)

				}
			} else {
				require.Equal(t, statement.Joins[0].Name, tc.wantSQL)
			}
		})
	}
}

func TestReadDB_GroupBy(t *testing.T) {
	tests := map[string]struct {
		name       string
		wantResult interface{}
	}{
		"group single column": {
			name:       "category",
			wantResult: "category",
		},
		"group multiple column": {
			name:       "category, type",
			wantResult: []string{"category, type"},
		},
		"group empty": {
			name:       "",
			wantResult: "",
		},
	}

	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			// given
			db, _, _ := setupTestReadDB(t)

			// when
			result := db.Group(tc.name)

			// then
			require.NoError(t, result.Error())
			statement := result.Statement()
			clauseGroup, ok := statement.Clauses["GROUP BY"]
			require.True(t, ok)

			groupClause, ok := clauseGroup.Expression.(clause.GroupBy)
			require.True(t, ok)

			if want, ok := tc.wantResult.([]string); ok {
				require.Len(t, groupClause.Columns, len(want))
				for i, col := range want {
					require.Equal(t, groupClause.Columns[i].Name, col)
				}
			} else {
				require.Equal(t, groupClause.Columns[0].Name, tc.wantResult.(string))
			}
		})
	}
}

func TestReadDB_Having(t *testing.T) {
	type args struct {
		query string
		args  []interface{}
	}
	tests := map[string]struct {
		args        args
		expectedSQL string
	}{
		"having count": {
			args: args{
				query: "COUNT(*) > ?",
				args:  []interface{}{1},
			},
			expectedSQL: "HAVING COUNT(*) > $1",
		},
		"having avg": {
			args: args{
				query: "AVG(age) >= ?",
				args:  []interface{}{10},
			},
			expectedSQL: "HAVING AVG(age) >= $1",
		},
		"having no args": {
			args: args{
				query: "SUM(score) > 50",
			},
			expectedSQL: "HAVING SUM(score) > 50",
		},
		"having with multiple conditions": {
			args: args{
				query: "COUNT(*) > ? AND SUM(score) < ?",
				args:  []interface{}{2, 100},
			},
			expectedSQL: "HAVING COUNT(*) > $1 AND SUM(score) < $2",
		},
	}

	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			db, _, _ := setupTestReadDB(t)

			statement := db.Session(&gorm.Session{DryRun: true}).
				Table("user_dummies").
				Select("role, count(*) as count").
				Group("role").
				Having(tc.args.query, tc.args.args...).
				Find(&[]UserDummy{}).
				Statement()

			sql := statement.SQL.String()
			require.Contains(t, sql, tc.expectedSQL)
			require.Equal(t, tc.args.args, statement.Vars)
		})
	}
}

func TestReadDB_OrderBy(t *testing.T) {
	tests := map[string]struct {
		order      interface{}
		wantResult string
	}{
		"simple string": {
			order:      "created_at desc, name",
			wantResult: "created_at desc, name",
		},
		"multi column": {
			order:      "name asc, created_at desc",
			wantResult: "name asc, created_at desc",
		},
		"gorm clause.OrderByColumn": {
			order: clause.OrderByColumn{
				Column: clause.Column{Name: "score"},
				Desc:   true,
			},
			wantResult: "score true",
		},
		"gorm clause.OrderBy multiple": {
			order: clause.OrderBy{
				Columns: []clause.OrderByColumn{
					{Column: clause.Column{Name: "age"}},
					{Column: clause.Column{Name: "score"}, Desc: true},
				},
			},
			wantResult: "age false, score true",
		},
	}

	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			// given
			db, _, _ := setupTestReadDB(t)

			// when
			result := db.Order(tc.order)

			// then
			require.NoError(t, result.Error())
			statement := result.Statement()
			statement.Scopes()

			clauseOrder, ok := statement.Clauses["ORDER BY"].Expression.(clause.OrderBy)
			require.True(t, ok, "ORDER BY clause not found")

			switch tc.order.(type) {
			case string:
				require.Equal(t, fmt.Sprint(clauseOrder.Columns[0].Column.Name), tc.wantResult)

			case clause.OrderByColumn:
				require.Equal(t, fmt.Sprint(clauseOrder.Columns[0].Column.Name, " ", clauseOrder.Columns[0].Desc), tc.wantResult)
			case clause.OrderBy:
				order := tc.order.(clause.OrderBy)
				str := ""
				for _, col := range order.Columns {
					str += fmt.Sprint(col.Column.Name, " ", col.Desc, ", ")
				}
				str = strings.Trim(str, ", ")
				require.Equal(t, str, tc.wantResult)
			}
		})
	}
}

func TestReadDB_LimitOffset(t *testing.T) {
	type args struct {
		query string
		args  []interface{}
	}
	tests := map[string]struct {
		args        args
		expectedSQL string
	}{
		"limit offset": {
			args: args{
				query: "LIMIT ? OFFSET ?",
				args:  []interface{}{1, 2},
			},
			expectedSQL: "LIMIT $1 OFFSET $2",
		},
	}

	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			db, _, _ := setupTestReadDB(t)

			var statement *gorm.Statement
			if tc.args.args != nil {
				limit := tc.args.args[0].(int)
				offset := tc.args.args[1].(int)
				statement = db.Session(&gorm.Session{DryRun: true}).
					Table("user_dummies").
					Select("role, count(*) as count").
					Limit(limit).Offset(offset).
					Find(&[]UserDummy{}).
					Statement()
			}

			sql := statement.SQL.String()
			require.Contains(t, sql, tc.expectedSQL)
			require.Equal(t, tc.args.args, statement.Vars)
		})
	}
}

func TestReadDB_Scopes(t *testing.T) {
	tests := map[string]struct {
		funcs       []func(db gormix.ReadOnlyDB) gormix.ReadOnlyDB
		expectedSQL string
	}{
		"scopes with order": {
			funcs: []func(db gormix.ReadOnlyDB) gormix.ReadOnlyDB{
				func(db gormix.ReadOnlyDB) gormix.ReadOnlyDB {
					return db.Order("name asc")
				},
			},
			expectedSQL: "ORDER BY name asc",
		},
		"multiple scopes": {
			funcs: []func(db gormix.ReadOnlyDB) gormix.ReadOnlyDB{
				func(db gormix.ReadOnlyDB) gormix.ReadOnlyDB {
					return db.Order("name asc")
				},
				func(db gormix.ReadOnlyDB) gormix.ReadOnlyDB {
					return db.Where("age > 10")
				},
			},
			expectedSQL: "WHERE age > 10 ORDER BY name asc",
		},
	}

	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			db, _, _ := setupTestReadDB(t)

			statement := db.Session(&gorm.Session{DryRun: true}).
				Scopes(tc.funcs...).
				Find(&[]UserDummy{}).
				Statement()

			sql := statement.SQL.String()
			require.Contains(t, sql, tc.expectedSQL)
		})
	}
}

func TestReadDB_Unscoped(t *testing.T) {
	tests := map[string]struct {
		unscoped    bool
		expectedCon string
	}{
		"unscoped true": {
			unscoped:    true,
			expectedCon: "deleted_at IS NULL",
		},
		"unscoped false": {
			unscoped:    false,
			expectedCon: "\"cities\".\"deleted_at\" IS NULL",
		},
	}

	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			db, _, _ := setupTestReadDB(t)

			var statement *gorm.Statement
			if tc.unscoped {
				statement = db.Session(&gorm.Session{DryRun: true}).Unscoped().Find(&[]City{}).Statement()
			} else {
				statement = db.Session(&gorm.Session{DryRun: true}).Find(&[]City{}).Statement()
			}

			sql := statement.SQL.String()
			fmt.Println("----------------", sql)
			if tc.unscoped {
				require.NotContains(t, sql, tc.expectedCon)
			} else {
				require.Contains(t, sql, tc.expectedCon)
			}
		})
	}
}

func TestReadDB_Preload(t *testing.T) {
	type args struct {
		query string
		args  []interface{}
	}
	tests := map[string]struct {
		preload  string
		args     args
		expected string
	}{
		"preload": {
			preload: "Orders",
		},
		"preload with condition": {
			preload: "Orders",
			args: args{
				query: "price > ?",
				args:  []interface{}{34.34},
			},
		},
	}

	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			db, _, _ := setupTestReadDB(t)

			statement := db.Session(&gorm.Session{DryRun: true}).
				Preload(tc.preload, tc.args.query, tc.args.args).
				Find(&[]UserDummy{}).Statement()
			preload := statement.Preloads
			fmt.Println("-----------", preload)

			for key, value := range preload {
				require.Equal(t, key, tc.preload)
				require.Equal(t, value, []interface{}{tc.args.query, tc.args.args})
			}

		})
	}
}

func TestReadDB_Distinct(t *testing.T) {
	tests := map[string]struct {
		distinct bool
	}{
		"distinct true": {
			distinct: true,
		},
		"distinct false": {
			distinct: false,
		},
	}

	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			db, _, _ := setupTestReadDB(t)
			result := db.Session(&gorm.Session{DryRun: true})
			if tc.distinct {
				result = db.Session(&gorm.Session{DryRun: true}).Distinct()
			}
			distinct := result.Statement().Distinct
			require.Equal(t, tc.distinct, distinct)
		})
	}
}

func TestReadDB_Omit(t *testing.T) {
	tests := map[string]struct {
		omitFields []string
		expected   string
	}{
		"omit single field": {
			omitFields: []string{"email"},
			expected:   "SELECT \"user_dummies\".\"id\",\"user_dummies\".\"name\" FROM \"user_dummies\"",
		},
		"omit multiple fields": {
			omitFields: []string{"email", "id"},
			expected:   "SELECT \"user_dummies\".\"name\" FROM \"user_dummies\"",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			db, _, _ := setupTestReadDB(t)

			statement := db.Session(&gorm.Session{DryRun: true}).
				Omit(tc.omitFields...).
				Find(&[]UserDummy{}).
				Statement()

			sql := statement.SQL.String()

			require.Equal(t, sql, tc.expected)
			for _, field := range tc.omitFields {
				require.NotContains(t, sql, field)
			}
		})
	}
}

func TestReadDB_Raw(t *testing.T) {
	tests := map[string]struct {
		rawQuery    string
		args        []interface{}
		expectedSQL string
	}{
		"simple raw query": {
			rawQuery:    "SELECT COUNT(*) FROM user_dummies",
			args:        nil,
			expectedSQL: "SELECT COUNT(*) FROM user_dummies",
		},
		"raw query with args": {
			rawQuery:    "SELECT * FROM user_dummies WHERE age > ?",
			args:        []interface{}{18},
			expectedSQL: "SELECT * FROM user_dummies WHERE age > $1",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			db, _, _ := setupTestReadDB(t)

			query := db.Session(&gorm.Session{DryRun: true}).
				Raw(tc.rawQuery, tc.args...)

			statement := query.Statement()

			sql := statement.SQL.String()
			require.Equal(t, sql, tc.expectedSQL)
			require.Equal(t, statement.Vars, tc.args)
		})
	}
}

func TestReadDB_Find(t *testing.T) {
	type args struct {
		query string
		args  []interface{}
	}
	tests := map[string]struct {
		setupMock   func(mock sqlmock.Sqlmock)
		args        args
		wantErr     bool
		typeOfErr   error
		expectedSQL string
		wantResult  []UserDummy
	}{
		"success": {
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := createDummyUsers(2)
				mock.ExpectQuery(regexp.QuoteMeta(
					`SELECT * FROM "user_dummies" WHERE id > $1`,
				)).WithArgs(0).
					WillReturnRows(rows)
			},
			args: args{
				query: "id > ?",
				args:  []interface{}{0},
			},
			wantErr:     false,
			expectedSQL: `SELECT * FROM "user_dummies" WHERE id > '[0]'`,
			wantResult: []UserDummy{
				{
					ID:    1,
					Name:  "User 1",
					Email: "Email1@example.com",
				},
				{
					ID:    2,
					Name:  "User 2",
					Email: "Email2@example.com",
				},
			},
		},
		"failure: record not found": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta(
					`SELECT * FROM "user_dummies" WHERE id < $1`,
				)).WithArgs(0).
					WillReturnError(sql.ErrNoRows)
			},
			args: args{
				query: "id < ?",
				args:  []interface{}{0},
			},
			wantErr:     true,
			typeOfErr:   sql.ErrNoRows,
			expectedSQL: `SELECT * FROM "user_dummies" WHERE id < '[0]'`,
			wantResult:  []UserDummy{},
		},
		"failure: query error": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM "user_dummies" WHERE id <= $1`)).
					WithArgs(0).
					WillReturnError(assert.AnError)
			},
			args: args{
				query: "id <= ?",
				args:  []interface{}{0},
			},
			expectedSQL: `SELECT * FROM "user_dummies" WHERE id <= '[0]'`,
			wantErr:     true,
			typeOfErr:   assert.AnError,
			wantResult:  []UserDummy{},
		},
	}

	for scenario, test := range tests {
		test := test
		t.Run(scenario, func(t *testing.T) {
			// Given
			db, mock, cleanup := setupTestReadDB(t)
			defer cleanup()
			test.setupMock(mock)

			// When
			result := []UserDummy{}
			query := db.Where(test.args.query, test.args.args...).Find(&result)
			checkQuery := db.Session(&gorm.Session{DryRun: true}).Where(test.args.query, test.args.args...).Find(&result)

			// Then
			err := query.Error()

			statement := checkQuery.Statement()
			dialector := checkQuery.Dialector()

			sql := statement.SQL.String()
			vars := statement.Vars

			if test.wantErr {
				require.Error(t, err)
				require.Equal(t, test.typeOfErr, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, test.expectedSQL, dialector.Explain(sql, vars))
			require.Equal(t, test.wantResult, result)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestReadDB_First(t *testing.T) {
	type args struct {
		query string
		args  []interface{}
	}
	tests := map[string]struct {
		setupMock    func(mock sqlmock.Sqlmock)
		args         args
		wantErr      bool
		typeOfErr    error
		expectedVars []interface{}
		wantResult   UserDummy
	}{
		"success": {
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := createDummyUsers(1)
				mock.ExpectQuery(regexp.QuoteMeta(
					`SELECT * FROM "user_dummies" WHERE id = $1 ORDER BY "user_dummies"."id" LIMIT $2`,
				)).WithArgs(1, 1).
					WillReturnRows(rows)
			},
			args: args{
				query: "id = ?",
				args:  []interface{}{1},
			},
			wantErr:      false,
			expectedVars: []interface{}{1, 1},
			wantResult: UserDummy{
				ID:    1,
				Name:  "User 1",
				Email: "Email1@example.com",
			},
		},
		"failure: not found": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta(
					`SELECT * FROM "user_dummies" WHERE email = $1 ORDER BY "user_dummies"."id" LIMIT $2`,
				)).WithArgs("notfound@example.com", 1).
					WillReturnError(sql.ErrNoRows)
			},
			args: args{
				query: "email = ?",
				args:  []interface{}{"notfound@example.com"},
			},
			wantErr:      true,
			expectedVars: []interface{}{"notfound@example.com", 1},
			typeOfErr:    sql.ErrNoRows,
			wantResult:   UserDummy{},
		},
	}

	for scenario, test := range tests {
		test := test
		t.Run(scenario, func(t *testing.T) {
			db, mock, cleanup := setupTestReadDB(t)
			defer cleanup()
			test.setupMock(mock)

			// thực thi truy vấn thật với sqlmock
			var result UserDummy
			query := db.Where(test.args.query, test.args.args...).First(&result)

			// dryrun để lấy SQL
			checkQuery := db.Session(&gorm.Session{DryRun: true}).Where(test.args.query, test.args.args...).First(&UserDummy{})
			statement := checkQuery.Statement()
			vars := statement.Vars

			err := query.Error()

			if test.wantErr {
				require.Error(t, err)
				require.Equal(t, test.typeOfErr, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, test.wantResult, result)
			require.Equal(t, test.expectedVars, vars)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestReadDB_Last(t *testing.T) {
	type args struct {
		query string
		args  []interface{}
	}
	tests := map[string]struct {
		setupMock    func(mock sqlmock.Sqlmock)
		args         args
		wantErr      bool
		typeOfErr    error
		expectedVars []interface{}
		wantResult   UserDummy
	}{
		"success": {
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := createDummyUsers(1)
				mock.ExpectQuery(regexp.QuoteMeta(
					`SELECT * FROM "user_dummies" WHERE id = $1 ORDER BY "user_dummies"."id" DESC LIMIT $2`,
				)).WithArgs(1, 1).
					WillReturnRows(rows)
			},
			args: args{
				query: "id = ?",
				args:  []interface{}{1},
			},
			wantErr:      false,
			expectedVars: []interface{}{1, 1},
			wantResult: UserDummy{
				ID:    1,
				Name:  "User 1",
				Email: "Email1@example.com",
			},
		},
		"failure: not found": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta(
					`SELECT * FROM "user_dummies" WHERE email = $1 ORDER BY "user_dummies"."id" DESC LIMIT $2`,
				)).WithArgs("notfound@example.com", 1).
					WillReturnError(sql.ErrNoRows)
			},
			args: args{
				query: "email = ?",
				args:  []interface{}{"notfound@example.com"},
			},
			wantErr:      true,
			expectedVars: []interface{}{"notfound@example.com", 1},
			typeOfErr:    sql.ErrNoRows,
			wantResult:   UserDummy{},
		},
	}

	for scenario, test := range tests {
		test := test
		t.Run(scenario, func(t *testing.T) {
			db, mock, cleanup := setupTestReadDB(t)
			defer cleanup()
			test.setupMock(mock)

			// thực thi truy vấn thật với sqlmock
			var result UserDummy
			query := db.Where(test.args.query, test.args.args...).Last(&result)

			// dryrun để lấy SQL
			checkQuery := db.Session(&gorm.Session{DryRun: true}).Where(test.args.query, test.args.args...).Last(&UserDummy{})
			statement := checkQuery.Statement()
			vars := statement.Vars

			err := query.Error()

			if test.wantErr {
				require.Error(t, err)
				require.Equal(t, test.typeOfErr, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, test.wantResult, result)
			require.Equal(t, test.expectedVars, vars)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestReadDB_Take(t *testing.T) {
	type args struct {
		query string
		args  []interface{}
	}
	tests := map[string]struct {
		setupMock    func(mock sqlmock.Sqlmock)
		args         args
		wantErr      bool
		typeOfErr    error
		expectedVars []interface{}
		wantResult   UserDummy
	}{
		"success": {
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := createDummyUsers(2)
				mock.ExpectQuery(regexp.QuoteMeta(
					`SELECT * FROM "user_dummies" WHERE id = $1 LIMIT $2`,
				)).WithArgs(1, 1).
					WillReturnRows(rows)
			},
			args: args{
				query: "id = ?",
				args:  []interface{}{1},
			},
			wantErr:      false,
			expectedVars: []interface{}{1, 1},
			wantResult: UserDummy{
				ID:    1,
				Name:  "User 1",
				Email: "Email1@example.com",
			},
		},
		"failure: not found": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta(
					`SELECT * FROM "user_dummies" WHERE email = $1 LIMIT $2`,
				)).WithArgs("notfound@example.com", 1).
					WillReturnError(sql.ErrNoRows)
			},
			args: args{
				query: "email = ?",
				args:  []interface{}{"notfound@example.com"},
			},
			wantErr:      true,
			expectedVars: []interface{}{"notfound@example.com", 1},
			typeOfErr:    sql.ErrNoRows,
			wantResult:   UserDummy{},
		},
	}

	for scenario, test := range tests {
		test := test
		t.Run(scenario, func(t *testing.T) {
			db, mock, cleanup := setupTestReadDB(t)
			defer cleanup()
			test.setupMock(mock)

			var result UserDummy
			query := db.Where(test.args.query, test.args.args...).Take(&result)

			// dryrun để lấy SQL
			checkQuery := db.Session(&gorm.Session{DryRun: true}).Where(test.args.query, test.args.args...).Take(&UserDummy{})
			statement := checkQuery.Statement()
			vars := statement.Vars

			err := query.Error()

			if test.wantErr {
				require.Error(t, err)
				require.Equal(t, test.typeOfErr, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, test.wantResult, result)
			require.Equal(t, test.expectedVars, vars)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestReadDB_Scan(t *testing.T) {
	type args struct {
		query string
		args  []interface{}
	}
	tests := map[string]struct {
		setupMock    func(mock sqlmock.Sqlmock)
		args         args
		wantErr      bool
		typeOfErr    error
		expectedVars []interface{}
		wantResult   []UserDummy
	}{
		"success": {
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := createDummyUsers(2)
				mock.ExpectQuery(regexp.QuoteMeta(
					`SELECT * FROM "user_dummies" WHERE id = $1`,
				)).WithArgs(1).
					WillReturnRows(rows)
			},
			args: args{
				query: "id = ?",
				args:  []interface{}{1},
			},
			wantErr:      false,
			expectedVars: []interface{}{1},
			wantResult: []UserDummy{
				{
					ID:    1,
					Name:  "User 1",
					Email: "Email1@example.com",
				},
				{
					ID:    2,
					Name:  "User 2",
					Email: "Email2@example.com",
				},
			},
		},
		"failure: not found": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta(
					`SELECT * FROM "user_dummies" WHERE email = $1`,
				)).WithArgs("notfound@example.com").
					WillReturnError(sql.ErrNoRows)
			},
			args: args{
				query: "email = ?",
				args:  []interface{}{"notfound@example.com"},
			},
			wantErr:      true,
			expectedVars: []interface{}{"notfound@example.com"},
			typeOfErr:    sql.ErrNoRows,
		},
		"failure: query error": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta(
					`SELECT * FROM "user_dummies" WHERE id = $1`,
				)).WithArgs(1).
					WillReturnError(assert.AnError)
			},
			args: args{
				query: "id = ?",
				args:  []interface{}{1},
			},
			wantErr:      true,
			expectedVars: []interface{}{1},
			typeOfErr:    assert.AnError,
		},
	}

	for scenario, test := range tests {
		test := test
		t.Run(scenario, func(t *testing.T) {
			db, mock, cleanup := setupTestReadDB(t)
			defer cleanup()
			test.setupMock(mock)

			var result []UserDummy
			query := db.Where(test.args.query, test.args.args...).Table("user_dummies").Scan(&result)

			checkQuery := db.Session(&gorm.Session{DryRun: true}).Where(test.args.query, test.args.args...).Table("user_dummies").Scan(&[]UserDummy{})
			statement := checkQuery.Statement()
			vars := statement.Vars

			err := query.Error()

			if test.wantErr {
				require.Error(t, err)
				require.Equal(t, test.typeOfErr, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, test.wantResult, result)
			require.Equal(t, test.expectedVars, vars)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestReadDB_Pluck(t *testing.T) {
	type args struct {
		column string
		query  string
		args   []interface{}
	}
	tests := map[string]struct {
		setupMock    func(mock sqlmock.Sqlmock)
		args         args
		wantErr      bool
		typeOfErr    error
		expectedVars []interface{}
		wantResult   []string
	}{
		"success: pluck emails where id > ?": {
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"email"}).
					AddRow("user1@example.com").
					AddRow("user2@example.com")
				mock.ExpectQuery(regexp.QuoteMeta(
					`SELECT "email" FROM "user_dummies" WHERE id > $1`,
				)).WithArgs(0).WillReturnRows(rows)
			},
			args: args{
				column: "email",
				query:  "id > ?",
				args:   []interface{}{0},
			},
			expectedVars: []interface{}{0},
			wantResult:   []string{"user1@example.com", "user2@example.com"},
		},
		"failure: no rows": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta(
					`SELECT "email" FROM "user_dummies" WHERE id < $1`,
				)).WithArgs(0).WillReturnError(sql.ErrNoRows)
			},
			args: args{
				column: "email",
				query:  "id < ?",
				args:   []interface{}{0},
			},
			expectedVars: []interface{}{0},
			wantErr:      true,
			typeOfErr:    sql.ErrNoRows,
		},
	}

	for scenario, test := range tests {
		test := test
		t.Run(scenario, func(t *testing.T) {
			db, mock, cleanup := setupTestReadDB(t)
			defer cleanup()
			test.setupMock(mock)

			var result []string
			query := db.Where(test.args.query, test.args.args...).
				Table("user_dummies").Pluck(test.args.column, &result)

			checkQuery := db.Session(&gorm.Session{DryRun: true}).
				Table("user_dummies").
				Where(test.args.query, test.args.args...).
				Pluck(test.args.column, &[]string{})
			statement := checkQuery.Statement()
			vars := statement.Vars

			err := query.Error()

			if test.wantErr {
				require.Error(t, err)
				require.Equal(t, test.typeOfErr, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, test.expectedVars, vars)
			require.Equal(t, test.wantResult, result)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestReadDB_Count(t *testing.T) {
	type args struct {
		query string
		args  []interface{}
	}
	tests := map[string]struct {
		setupMock  func(mock sqlmock.Sqlmock)
		args       args
		wantErr    bool
		typeOfErr  error
		wantResult int64
	}{
		"success": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta(`SELECT count(*) FROM "user_dummies" WHERE id > $1`)).
					WithArgs(0).
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))
			},
			args: args{
				query: "id > ?",
				args:  []interface{}{0},
			},
			wantErr:    false,
			wantResult: 3,
		},
		"failure: query error": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta(`SELECT count(*) FROM "user_dummies" WHERE id <= $1`)).
					WithArgs(0).
					WillReturnError(assert.AnError)
			},
			args: args{
				query: "id <= ?",
				args:  []interface{}{0},
			},
			wantErr:   true,
			typeOfErr: assert.AnError,
		},
	}

	for scenario, test := range tests {
		test := test
		t.Run(scenario, func(t *testing.T) {
			db, mock, cleanup := setupTestReadDB(t)
			defer cleanup()
			test.setupMock(mock)

			var count int64
			var count2 int64
			query := db.Model(&UserDummy{}).
				Where(test.args.query, test.args.args...).
				Count(&count)

			checkQuery := db.Session(&gorm.Session{DryRun: true}).
				Model(&UserDummy{}).
				Where(test.args.query, test.args.args...).
				Count(&count2)
			vars := checkQuery.Statement().Vars

			err := query.Error()

			if test.wantErr {
				require.Error(t, err)
				require.Equal(t, test.typeOfErr, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, test.args.args, vars)
			require.Equal(t, test.wantResult, count)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestReadDB_Row(t *testing.T) {
	tests := map[string]struct {
		setupMock    func(mock sqlmock.Sqlmock)
		query        string
		args         []interface{}
		expectedScan []interface{}
		wantErr      bool
		typeOfErr    error
	}{
		"success: select email by id": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta(`SELECT email FROM user_dummies WHERE id = $1`)).
					WithArgs(1).
					WillReturnRows(sqlmock.NewRows([]string{"email"}).AddRow("user@example.com"))
			},
			query:        "SELECT email FROM user_dummies WHERE id = $1",
			args:         []interface{}{1},
			expectedScan: []interface{}{"user@example.com"},
			wantErr:      false,
		},
		"failure: no row": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta(`SELECT email FROM user_dummies WHERE id = $1`)).
					WithArgs(2).
					WillReturnError(sql.ErrNoRows)
			},
			query:     "SELECT email FROM user_dummies WHERE id = $1",
			args:      []interface{}{2},
			wantErr:   true,
			typeOfErr: sql.ErrNoRows,
		},
	}

	for scenario, test := range tests {
		test := test
		t.Run(scenario, func(t *testing.T) {
			db, mock, cleanup := setupTestReadDB(t)
			defer cleanup()
			test.setupMock(mock)

			row := db.Raw(test.query, test.args...).Row()
			var email string
			err := row.Scan(&email)
			if test.wantErr {
				require.Error(t, err)
				require.Equal(t, test.typeOfErr, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expectedScan[0].(string), email)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestReadDB_Rows(t *testing.T) {
	tests := map[string]struct {
		setupMock     func(mock sqlmock.Sqlmock)
		query         string
		args          []interface{}
		expectedRows  []string
		wantErr       bool
		expectedError error
	}{
		"success: select emails": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta(`SELECT email FROM user_dummies WHERE id > $1`)).
					WithArgs(0).
					WillReturnRows(sqlmock.NewRows([]string{"email"}).
						AddRow("user1@example.com").
						AddRow("user2@example.com"))
			},
			query:        "SELECT email FROM user_dummies WHERE id > $1",
			args:         []interface{}{0},
			expectedRows: []string{"user1@example.com", "user2@example.com"},
		},
		"failure: query error": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta(`SELECT email FROM user_dummies WHERE id < $1`)).
					WithArgs(0).
					WillReturnError(sql.ErrConnDone)
			},
			query:         "SELECT email FROM user_dummies WHERE id < $1",
			args:          []interface{}{0},
			wantErr:       true,
			expectedError: sql.ErrConnDone,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			db, mock, cleanup := setupTestReadDB(t)
			defer cleanup()
			test.setupMock(mock)

			rows, err := db.Raw(test.query, test.args...).Rows()
			if test.wantErr {
				require.Error(t, err)
				require.Equal(t, test.expectedError, err)
				return
			}

			defer rows.Close()
			var results []string
			for rows.Next() {
				var email string
				err := rows.Scan(&email)
				require.NoError(t, err)
				results = append(results, email)
			}

			require.Equal(t, test.expectedRows, results)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
