package postgres

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func MigrateUp(databaseURL, migrationsPath string) error {
	url := toPostgresMigrateURL(databaseURL)
	var last error
	for i := 0; i < 20; i++ {
		m, err := migrate.New("file://"+migrationsPath, url)
		if err != nil {
			last = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		err = m.Up()
		_, _ = m.Close()
		if err == nil || errors.Is(err, migrate.ErrNoChange) {
			return nil
		}
		last = err
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("migrate up: %w", last)
}

func toPostgresMigrateURL(dsn string) string {
	// lib/pq / database/postgres driver expects postgres://
	if strings.HasPrefix(dsn, "pgx5://") {
		return "postgres://" + strings.TrimPrefix(dsn, "pgx5://")
	}
	return dsn
}
