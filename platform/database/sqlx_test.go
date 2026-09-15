package database

import "testing"

func TestSQLXDriverName(t *testing.T) {
	name, err := sqlxDriverName(Postgres)
	if err != nil || name != "pgx" {
		t.Fatalf("postgres = %s %v", name, err)
	}
	name, err = sqlxDriverName(MySQL)
	if err != nil || name != "mysql" {
		t.Fatalf("mysql = %s %v", name, err)
	}
	if _, err = sqlxDriverName("sqlite"); err == nil {
		t.Fatal("expected error for sqlite")
	}
}
