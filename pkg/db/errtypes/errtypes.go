package errtypes

import (
	"errors"

	sqlite3 "github.com/glebarez/go-sqlite"
	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
)

func IsUniqueConstraintErr(err error) bool {
	if mysqlErr := (*mysql.MySQLError)(nil); errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 { // error 1062 is a duplicate entry error
		return true
	}
	if sqliteErr := (*sqlite3.Error)(nil); errors.As(err, &sqliteErr) && (sqliteErr.Code() == 19 || sqliteErr.Code() == 2067) {
		// error 19 is a duplicate entry error and error 2067 is a unique constraint error
		return true
	}
	if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) && pgErr.Code == "23505" { // error 23505 is a unique violation error
		return true
	}
	return false
}
