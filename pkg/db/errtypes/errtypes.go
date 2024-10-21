package errtypes

import (
	"errors"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	sqlite3 "modernc.org/sqlite"
)

func IsUniqueConstraintErr(err error) bool {
	if mysqlErr := (*mysql.MySQLError)(nil); errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 { // error 1062 is a duplicate entry error
		return true
	}
	if sqliteErr := (*sqlite3.Error)(nil); errors.As(err, &sqliteErr) && sqliteErr.Code() == 19 { // error 19 is a duplicate entry error
		return true
	}
	if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) && pgErr.Code == "23505" { // error 23505 is a unique violation error
		return true
	}
	return false
}