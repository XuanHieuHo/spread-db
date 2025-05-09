package test

import (
	"context"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XuanHieuHo/spread-db/gormix"
	"github.com/XuanHieuHo/spread-db/gormix/provider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"regexp"
	"testing"
	"time"
)

type Animal struct {
	ID        int64     `gorm:"column:id;type:int64;primaryKey" json:"id"`
	Name      string    `gorm:"column:name;type:varchar(255);not null" json:"name"`
	UpdatedAt time.Time `gorm:"column:updated_at;type:timestamp with time zone;not null;default:now()" json:"updated_at"`
}

func setupTestWriteDB(t *testing.T) (gormix.WriteOnlyDB, sqlmock.Sqlmock, func()) {
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
	dbProvider := provider.NewDBProvider(&gorm.DB{}, db)

	return dbProvider.Write, mock, cleanup
}

func TestWriteDB_Create(t *testing.T) {
	tests := map[string]struct {
		setupMock  func(mock sqlmock.Sqlmock)
		value      UserDummy
		wantErr    bool
		typeOfErr  error
		wantResult UserDummy
	}{
		"success": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO "user_dummies" ("name","email") VALUES ($1,$2) RETURNING "id"`)).
					WithArgs("User 1", "Email1@example.com").
					WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
				mock.ExpectCommit()
			},
			value: UserDummy{
				Name:  "User 1",
				Email: "Email1@example.com",
			},
			wantErr: false,
			wantResult: UserDummy{
				ID:    1,
				Name:  "User 1",
				Email: "Email1@example.com",
			},
		},
		"failure: query error": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO "user_dummies" ("name","email") VALUES ($1,$2) RETURNING "id"`)).
					WithArgs("User 1", "Email1@example.com").
					WillReturnError(assert.AnError)
				mock.ExpectRollback()
			},
			value: UserDummy{
				Name:  "User 1",
				Email: "Email1@example.com",
			},
			wantErr:   true,
			typeOfErr: assert.AnError,
		},
	}

	for scenario, test := range tests {
		test := test
		t.Run(scenario, func(t *testing.T) {
			// Given
			db, mock, cleanup := setupTestWriteDB(t)
			defer cleanup()
			test.setupMock(mock)
			ctx := context.Background()

			// When
			user := UserDummy{
				Name:  "User 1",
				Email: "Email1@example.com",
			}

			err := db.WithContext(ctx).Create(&user).Error()

			// Then
			if test.wantErr {
				require.Error(t, err)
				require.Equal(t, test.typeOfErr, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.wantResult, user)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestWriteDB_CreateInBatches(t *testing.T) {
	tests := map[string]struct {
		setupMock  func(mock sqlmock.Sqlmock)
		values     []UserDummy
		batchSize  int
		wantErr    bool
		typeOfErr  error
		wantResult []UserDummy
	}{
		"success: insert in batches": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta(
					`INSERT INTO "user_dummies" ("name","email") VALUES ($1,$2),($3,$4) RETURNING "id"`)).
					WithArgs("User 1", "Email1@example.com", "User 2", "Email2@example.com").
					WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1).AddRow(2))
				mock.ExpectCommit()
			},
			values: []UserDummy{
				{Name: "User 1", Email: "Email1@example.com"},
				{Name: "User 2", Email: "Email2@example.com"},
			},
			batchSize: 2,
			wantErr:   false,
			wantResult: []UserDummy{
				{ID: 1, Name: "User 1", Email: "Email1@example.com"},
				{ID: 2, Name: "User 2", Email: "Email2@example.com"},
			},
		},
		"success: batch size smaller than total records": {
			setupMock: func(mock sqlmock.Sqlmock) {
				// First batch
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta(
					`INSERT INTO "user_dummies" ("name","email") VALUES ($1,$2),($3,$4),($5,$6),($7,$8),($9,$10),($11,$12),($13,$14),($15,$16),($17,$18) RETURNING "id"`)).
					WithArgs("User 1", "Email1@example.com", "User 2", "Email2@example.com", "User 3", "Email3@example.com", "User 4", "Email4@example.com", "User 5", "Email5@example.com", "User 6", "Email6@example.com", "User 7", "Email7@example.com", "User 8", "Email8@example.com", "User 9", "Email9@example.com").
					WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1).AddRow(2).AddRow(3).AddRow(4).AddRow(5).AddRow(6).AddRow(7).AddRow(8).AddRow(9))

				// Second batch
				mock.ExpectQuery(regexp.QuoteMeta(
					`INSERT INTO "user_dummies" ("name","email") VALUES ($1,$2) RETURNING "id"`)).
					WithArgs("User 10", "Email10@example.com").
					WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(10))
				mock.ExpectCommit()
			},
			values: []UserDummy{
				{Name: "User 1", Email: "Email1@example.com"},
				{Name: "User 2", Email: "Email2@example.com"},
				{Name: "User 3", Email: "Email3@example.com"},
				{Name: "User 4", Email: "Email4@example.com"},
				{Name: "User 5", Email: "Email5@example.com"},
				{Name: "User 6", Email: "Email6@example.com"},
				{Name: "User 7", Email: "Email7@example.com"},
				{Name: "User 8", Email: "Email8@example.com"},
				{Name: "User 9", Email: "Email9@example.com"},
				{Name: "User 10", Email: "Email10@example.com"},
			},
			batchSize: 9,
			wantErr:   false,
			wantResult: []UserDummy{
				{ID: 1, Name: "User 1", Email: "Email1@example.com"},
				{ID: 2, Name: "User 2", Email: "Email2@example.com"},
				{ID: 3, Name: "User 3", Email: "Email3@example.com"},
				{ID: 4, Name: "User 4", Email: "Email4@example.com"},
				{ID: 5, Name: "User 5", Email: "Email5@example.com"},
				{ID: 6, Name: "User 6", Email: "Email6@example.com"},
				{ID: 7, Name: "User 7", Email: "Email7@example.com"},
				{ID: 8, Name: "User 8", Email: "Email8@example.com"},
				{ID: 9, Name: "User 9", Email: "Email9@example.com"},
				{ID: 10, Name: "User 10", Email: "Email10@example.com"},
			},
		},
		"failure: insert error": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta(
					`INSERT INTO "user_dummies" ("name","email") VALUES ($1,$2),($3,$4) RETURNING "id"`)).
					WithArgs("User 1", "Email1@example.com", "User 2", "Email2@example.com").
					WillReturnError(assert.AnError)
				mock.ExpectRollback()
			},
			values: []UserDummy{
				{Name: "User 1", Email: "Email1@example.com"},
				{Name: "User 2", Email: "Email2@example.com"},
			},
			batchSize: 2,
			wantErr:   true,
			typeOfErr: assert.AnError,
		},
	}

	for scenario, test := range tests {
		test := test
		t.Run(scenario, func(t *testing.T) {
			// Given
			db, mock, cleanup := setupTestWriteDB(t)
			defer cleanup()
			test.setupMock(mock)
			ctx := context.Background()

			records := make([]UserDummy, len(test.values))
			copy(records, test.values)

			// when
			err := db.WithContext(ctx).CreateInBatches(&records, test.batchSize).Error()

			// Then
			if test.wantErr {
				require.Error(t, err)
				require.Equal(t, test.typeOfErr, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.wantResult, records)
				require.Equal(t, len(test.wantResult), len(records))
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestWriteDB_Save(t *testing.T) {
	tests := map[string]struct {
		setupMock  func(mock sqlmock.Sqlmock)
		value      UserDummy
		wantErr    bool
		typeOfErr  error
		wantResult UserDummy
	}{
		"success": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`UPDATE "user_dummies" SET "name"=$1,"email"=$2 WHERE "id" = $3`)).
					WithArgs("User 2", "Email2@example.com", 1).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()
			},
			value: UserDummy{
				ID:    1,
				Name:  "User 2",
				Email: "Email2@example.com",
			},
			wantErr: false,
			wantResult: UserDummy{
				ID:    1,
				Name:  "User 2",
				Email: "Email2@example.com",
			},
		},
		"failure: query error": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`UPDATE "user_dummies" SET "name"=$1,"email"=$2 WHERE "id" = $3`)).
					WithArgs("User 2", "Email2@example.com", 1).
					WillReturnError(assert.AnError)
				mock.ExpectRollback()
			},
			value: UserDummy{
				ID:    1,
				Name:  "User 2",
				Email: "Email2@example.com",
			},
			wantErr:   true,
			typeOfErr: assert.AnError,
			wantResult: UserDummy{
				ID:    1,
				Name:  "User 1",
				Email: "Email1@example.com",
			},
		},
	}

	for scenario, test := range tests {
		test := test
		t.Run(scenario, func(t *testing.T) {
			// Given
			db, mock, cleanup := setupTestWriteDB(t)
			defer cleanup()
			test.setupMock(mock)
			ctx := context.Background()

			// When
			originalUser := UserDummy{
				ID:    1,
				Name:  "User 1",
				Email: "Email1@example.com",
			}
			inputUser := originalUser
			inputUser.Name = "User 2"
			inputUser.Email = "Email2@example.com"

			// When
			err := db.WithContext(ctx).Save(&inputUser).Error()

			// Then
			if test.wantErr {
				require.Error(t, err)
				require.Equal(t, test.typeOfErr, err)
				require.Equal(t, test.wantResult, originalUser)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.wantResult, inputUser)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestWriteDB_Update(t *testing.T) {
	timeNow := time.Now()
	type args struct {
		query string
		args  []interface{}
	}
	tests := map[string]struct {
		setupMock  func(mock sqlmock.Sqlmock)
		column     string
		value      interface{}
		model      interface{}
		args       args
		wantErr    bool
		typeOfErr  error
		wantResult Animal
	}{
		"success": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`UPDATE "animals" SET "name"=$1,"updated_at"=$2 WHERE id = $3`)).
					WithArgs("Updated Name", sqlmock.AnyArg(), 1).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()

				mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM "animals" WHERE id = $1`)).
					WithArgs(1).
					WillReturnRows(sqlmock.NewRows([]string{"id", "name", "updated_at"}).
						AddRow(1, "Updated Name", timeNow))
			},
			column: "name",
			value:  "Updated Name",
			model:  &Animal{},
			args: args{
				query: "id = ?",
				args:  []interface{}{1},
			},
			wantErr: false,
			wantResult: Animal{
				ID:        1,
				Name:      "Updated Name",
				UpdatedAt: timeNow,
			},
		},
		"failure: update error": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`UPDATE "animals" SET "name"=$1,"updated_at"=$2 WHERE id = $3`)).
					WithArgs("Updated Name", sqlmock.AnyArg(), 1).
					WillReturnError(assert.AnError)
				mock.ExpectRollback()
			},
			column: "name",
			value:  "Updated Name",
			model:  &Animal{},
			args: args{
				query: "id = ?",
				args:  []interface{}{1},
			},
			wantErr:   true,
			typeOfErr: assert.AnError,
			wantResult: Animal{
				ID:        1,
				Name:      "Animal 1",
				UpdatedAt: timeNow.Add(-1 * time.Minute),
			},
		},
	}

	for scenario, test := range tests {
		test := test
		t.Run(scenario, func(t *testing.T) {
			// Given
			db, mock, cleanup := setupTestWriteDB(t)
			defer cleanup()
			test.setupMock(mock)
			ctx := context.Background()

			originalAnimal := Animal{
				ID:        1,
				Name:      "Animal 1",
				UpdatedAt: timeNow.Add(-1 * time.Minute),
			}

			// When
			err := db.WithContext(ctx).Model(test.model).Where(test.args.query, test.args.args...).Update(test.column, test.value).Scan(&originalAnimal).Error()

			// Then
			if test.wantErr {
				require.Error(t, err)
				require.Equal(t, test.typeOfErr, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, test.wantResult, originalAnimal)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestWriteDB_Updates(t *testing.T) {
	timeNow := time.Now()

	type args struct {
		query string
		args  []interface{}
	}
	tests := map[string]struct {
		setupMock  func(mock sqlmock.Sqlmock)
		value      map[string]interface{}
		model      interface{}
		args       args
		wantErr    bool
		typeOfErr  error
		wantResult Animal
	}{
		"success": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`UPDATE "animals" SET "name"=$1,"updated_at"=$2 WHERE id = $3`)).
					WithArgs("Updated Animal", sqlmock.AnyArg(), 1).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()

				mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM "animals" WHERE id = $1`)).
					WithArgs(1).
					WillReturnRows(sqlmock.NewRows([]string{"id", "name", "updated_at"}).
						AddRow(1, "Updated Animal", timeNow))
			},
			value: map[string]interface{}{
				"name": "Updated Animal",
			},
			model: &Animal{},
			args: args{
				query: "id = ?",
				args:  []interface{}{1},
			},
			wantErr: false,
			wantResult: Animal{
				ID:        1,
				Name:      "Updated Animal",
				UpdatedAt: timeNow,
			},
		},
		"failure: update error": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`UPDATE "animals" SET "name"=$1,"updated_at"=$2 WHERE id = $3`)).
					WithArgs("Updated Animal", sqlmock.AnyArg(), 1).
					WillReturnError(assert.AnError)
				mock.ExpectRollback()
			},
			value: map[string]interface{}{
				"name": "Updated Animal",
			},
			model: &Animal{},
			args: args{
				query: "id = ?",
				args:  []interface{}{1},
			},
			wantErr:   true,
			typeOfErr: assert.AnError,
			wantResult: Animal{
				ID:        1,
				Name:      "Animal 1",
				UpdatedAt: timeNow.Add(-1 * time.Minute),
			},
		},
	}

	for scenario, test := range tests {
		test := test
		t.Run(scenario, func(t *testing.T) {
			// Given
			db, mock, cleanup := setupTestWriteDB(t)
			defer cleanup()
			test.setupMock(mock)
			ctx := context.Background()

			originalAnimal := Animal{
				ID:        1,
				Name:      "Animal 1",
				UpdatedAt: timeNow.Add(-1 * time.Minute),
			}

			// When
			err := db.WithContext(ctx).Model(test.model).Where(test.args.query, test.args.args...).Updates(test.value).Scan(&originalAnimal).Error()

			// Then
			if test.wantErr {
				require.Error(t, err)
				require.Equal(t, test.typeOfErr, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, test.wantResult, originalAnimal)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestWriteDB_UpdateColumn(t *testing.T) {
	timeNow := time.Now()
	type args struct {
		query string
		args  []interface{}
	}
	tests := map[string]struct {
		setupMock  func(mock sqlmock.Sqlmock)
		column     string
		value      interface{}
		model      interface{}
		args       args
		wantErr    bool
		typeOfErr  error
		wantResult Animal
	}{
		"success: only name updated, not updated_at": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`UPDATE "animals" SET "name"=$1 WHERE id = $2`)).
					WithArgs("Updated Name", 1).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()

				mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM "animals" WHERE id = $1`)).
					WithArgs(1).
					WillReturnRows(sqlmock.NewRows([]string{"id", "name", "updated_at"}).
						AddRow(1, "Updated Name", timeNow.Add(-1*time.Minute)))
			},
			column: "name",
			value:  "Updated Name",
			model:  &Animal{},
			args: args{
				query: "id = ?",
				args:  []interface{}{1},
			},
			wantErr: false,
			wantResult: Animal{
				ID:        1,
				Name:      "Updated Name",
				UpdatedAt: timeNow.Add(-1 * time.Minute),
			},
		},
		"failure: update column error": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`UPDATE "animals" SET "name"=$1 WHERE id = $2`)).
					WithArgs("Updated Name", 1).
					WillReturnError(assert.AnError)
				mock.ExpectRollback()
			},
			column: "name",
			value:  "Updated Name",
			model:  &Animal{},
			args: args{
				query: "id = ?",
				args:  []interface{}{1},
			},
			wantErr:   true,
			typeOfErr: assert.AnError,
			wantResult: Animal{
				ID:        1,
				Name:      "Animal 1",
				UpdatedAt: timeNow.Add(-1 * time.Minute),
			},
		},
	}

	for scenario, test := range tests {
		test := test
		t.Run(scenario, func(t *testing.T) {
			db, mock, cleanup := setupTestWriteDB(t)
			defer cleanup()
			test.setupMock(mock)
			ctx := context.Background()

			originalAnimal := Animal{
				ID:        1,
				Name:      "Animal 1",
				UpdatedAt: timeNow.Add(-1 * time.Minute),
			}

			// When
			err := db.WithContext(ctx).Model(test.model).Where(test.args.query, test.args.args...).UpdateColumn(test.column, test.value).Scan(&originalAnimal).Error()

			if test.wantErr {
				require.Error(t, err)
				require.Equal(t, test.typeOfErr, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, test.wantResult, originalAnimal)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestWriteDB_UpdateColumns(t *testing.T) {
	timeNow := time.Now()

	type args struct {
		query string
		args  []interface{}
	}
	tests := map[string]struct {
		setupMock  func(mock sqlmock.Sqlmock)
		value      map[string]interface{}
		model      interface{}
		args       args
		wantErr    bool
		typeOfErr  error
		wantResult Animal
	}{
		"success": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`UPDATE "animals" SET "name"=$1 WHERE id = $2`)).
					WithArgs("Updated Animal", 1).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()

				mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM "animals" WHERE id = $1`)).
					WithArgs(1).
					WillReturnRows(sqlmock.NewRows([]string{"id", "name", "updated_at"}).
						AddRow(1, "Updated Animal", timeNow.Add(-1*time.Minute)))
			},
			value: map[string]interface{}{
				"name": "Updated Animal",
			},
			model: &Animal{},
			args: args{
				query: "id = ?",
				args:  []interface{}{1},
			},
			wantErr: false,
			wantResult: Animal{
				ID:        1,
				Name:      "Updated Animal",
				UpdatedAt: timeNow.Add(-1 * time.Minute),
			},
		},
		"failure: update error": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`UPDATE "animals" SET "name"=$1 WHERE id = $2`)).
					WithArgs("Updated Animal", 1).
					WillReturnError(assert.AnError)
				mock.ExpectRollback()
			},
			value: map[string]interface{}{
				"name": "Updated Animal",
			},
			model: &Animal{},
			args: args{
				query: "id = ?",
				args:  []interface{}{1},
			},
			wantErr:   true,
			typeOfErr: assert.AnError,
			wantResult: Animal{
				ID:        1,
				Name:      "Animal 1",
				UpdatedAt: timeNow.Add(-1 * time.Minute),
			},
		},
	}

	for scenario, test := range tests {
		test := test
		t.Run(scenario, func(t *testing.T) {
			// Given
			db, mock, cleanup := setupTestWriteDB(t)
			defer cleanup()
			test.setupMock(mock)
			ctx := context.Background()

			originalAnimal := Animal{
				ID:        1,
				Name:      "Animal 1",
				UpdatedAt: timeNow.Add(-1 * time.Minute),
			}

			// When
			err := db.WithContext(ctx).Model(test.model).Where(test.args.query, test.args.args...).UpdateColumns(test.value).Scan(&originalAnimal).Error()

			// Then
			if test.wantErr {
				require.Error(t, err)
				require.Equal(t, test.typeOfErr, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, test.wantResult, originalAnimal)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestWriteDB_Delete(t *testing.T) {
	timeNow := time.Now()
	type args struct {
		query string
		args  []interface{}
	}

	tests := map[string]struct {
		setupMock  func(mock sqlmock.Sqlmock)
		model      interface{}
		args       args
		wantResult City
		wantErr    bool
		typeOfErr  error
	}{
		"success": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`UPDATE "cities" SET "deleted_at"=$1 WHERE id = $2 AND "cities"."deleted_at" IS NULL`)).
					WithArgs(sqlmock.AnyArg(), 1).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()

				mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM "cities" WHERE "cities"."id" = $1`)).
					WithArgs(1).
					WillReturnRows(sqlmock.NewRows([]string{"id", "name", "deleted_at"}).
						AddRow(1, "City 1", timeNow))
			},
			model: &City{},
			args: args{
				query: "id = ?",
				args:  []interface{}{1},
			},
			wantResult: City{
				ID:        1,
				Name:      "City 1",
				DeletedAt: gorm.DeletedAt{Time: timeNow, Valid: true},
			},
			wantErr: false,
		},
		"failure: delete error": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`UPDATE "cities" SET "deleted_at"=$1 WHERE id = $2 AND "cities"."deleted_at" IS NULL`)).
					WithArgs(sqlmock.AnyArg(), 1).
					WillReturnError(assert.AnError)
				mock.ExpectRollback()

			},
			model: &City{},
			args: args{
				query: "id = ?",
				args:  []interface{}{1},
			},
			wantResult: City{
				ID:   1,
				Name: "City 1",
			},
			wantErr:   true,
			typeOfErr: assert.AnError,
		},
	}

	for scenario, test := range tests {
		test := test
		t.Run(scenario, func(t *testing.T) {
			// Given
			db, mock, cleanup := setupTestWriteDB(t)
			defer cleanup()
			test.setupMock(mock)
			ctx := context.Background()

			// When
			originalCity := City{
				ID:   1,
				Name: "City 1",
			}

			// When
			err := db.WithContext(ctx).Where(test.args.query, test.args.args...).Delete(test.model).Error()

			// Then
			if test.wantErr {
				require.Error(t, err)
				require.Equal(t, test.typeOfErr, err)
			} else {
				require.NoError(t, err)
				err = db.WithContext(ctx).Unscoped().Find(&originalCity).Error()
				require.NoError(t, err)
			}
			require.Equal(t, test.wantResult, originalCity)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestWriteDB_Transaction(t *testing.T) {
	tests := map[string]struct {
		setupMock func(mock sqlmock.Sqlmock)
		txFunc    func(tx gormix.WriteOnlyDB) error
		wantErr   bool
	}{
		"success: transaction committed": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO "cities" ("name") VALUES ($1)`)).
					WithArgs("Hanoi").
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()
			},
			txFunc: func(tx gormix.WriteOnlyDB) error {
				return tx.Exec(`INSERT INTO "cities" ("name") VALUES (?)`, "Hanoi").Error()
			},
			wantErr: false,
		},
		"failure: transaction rolled back": {
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO "cities" ("name") VALUES ($1)`)).
					WithArgs("Saigon").
					WillReturnError(assert.AnError)
				mock.ExpectRollback()
			},
			txFunc: func(tx gormix.WriteOnlyDB) error {
				return tx.Exec(`INSERT INTO "cities" ("name") VALUES (?)`, "Saigon").Error()
			},
			wantErr: true,
		},
	}

	for scenario, test := range tests {
		test := test
		t.Run(scenario, func(t *testing.T) {
			db, mock, cleanup := setupTestWriteDB(t)
			defer cleanup()

			test.setupMock(mock)

			err := db.Transaction(test.txFunc)
			if test.wantErr {
				require.Error(t, err)
				require.Equal(t, assert.AnError, err)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
