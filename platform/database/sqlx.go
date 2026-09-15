package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // load pgx driver for PostgreSQL
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
	sqldblogger "github.com/simukti/sqldb-logger"
	"github.com/simukti/sqldb-logger/logadapter/zerologadapter"
)

var (
	SqlxConn *SqlxDatabase
	sqlxOnce sync.Once
)

type SqlxDatabase struct {
	*sqlx.DB
}

type Columns struct {
	FieldName  string `db:"fieldname" json:"field_name"`
	DataType   string `db:"datatype" json:"data_type"`
	MaxLength  int    `db:"maxlength" json:"max_length"`
	IsIdentity string `db:"isidentity" json:"is_identity"`
	IsNullable string `db:"isnullable" json:"is_nullable"`
	Extra      string `db:"extra" json:"extra"`
}

var columnDB = map[string][]*Columns{}

// TODO: not allow logging database query in production
// NewDatabaseConn to get connection to postgresql
func NewDatabaseConn(dbUri string) *SqlxDatabase {
	sqlxOnce.Do(func() {
		driverName := "pgx"

		db, err := sql.Open(driverName, dbUri)
		if err != nil {
			panic(err)
		}

		db = sqldblogger.OpenDriver(dbUri, db.Driver(), zerologadapter.New(zerolog.New(os.Stdout)))

		dbSqlx := sqlx.NewDb(db, driverName)

		// connection pool settings
		db.SetMaxOpenConns(50)
		db.SetMaxIdleConns(10)
		db.SetConnMaxLifetime(30 * time.Minute)
		db.SetConnMaxIdleTime(5 * time.Minute)

		// Try to ping database.
		SqlxConn = &SqlxDatabase{dbSqlx}
		if err := dbSqlx.Ping(); err != nil {
			defer dbSqlx.Close()

			panic(err)
		}
	})

	return SqlxConn
}

func (e *SqlxDatabase) Begin() (*sqlx.Tx, error) {
	tx, err := e.DB.Beginx()
	if err != nil {
		return nil, err
	}
	return tx, nil
}

func (e *SqlxDatabase) BeginReadCommitted(ctx context.Context) (*sqlx.Tx, error) {
	return e.DB.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
}

func (e *SqlxDatabase) BeginRepeatableRead(ctx context.Context) (*sqlx.Tx, error) {
	return e.DB.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
}

func (e *SqlxDatabase) BeginSerializable(ctx context.Context) (*sqlx.Tx, error) {
	return e.DB.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
}

func (e *SqlxDatabase) BeginReadOnly(ctx context.Context) (*sqlx.Tx, error) {
	return e.DB.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  true,
	})
}

func (e *SqlxDatabase) BeginRepeatableReadOnly(ctx context.Context) (*sqlx.Tx, error) {
	return e.DB.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
}

func (e *SqlxDatabase) Commit(tx *sqlx.Tx) error {
	if tx != nil {
		err := tx.Commit()
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *SqlxDatabase) RollBack(tx *sqlx.Tx) error {
	if tx != nil {
		if err := tx.Rollback(); err != nil {
			return err
		}
	}
	return nil
}

func (e *SqlxDatabase) InsertObject(tableName string, object interface{}) (int, error) {
	var lastID int

	// Get table columns first to validate and structure the data
	columns, err := e.getTableColumns(tableName)
	if err != nil {
		return 0, fmt.Errorf("failed to get table columns: %w", err)
	}

	// Convert object to map
	jsonData, err := json.Marshal(object)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal object: %w", err)
	}

	data := make(map[string]interface{})
	if err := json.Unmarshal(jsonData, &data); err != nil {
		return 0, fmt.Errorf("failed to unmarshal to map: %w", err)
	}

	// Pre-allocate slices and maps
	columnNames := make([]string, 0, len(columns))
	placeholders := make([]string, 0, len(columns))
	params := make(map[string]interface{}, len(columns))

	// Build query components
	for _, col := range columns {
		// Skip auto-incrementing identity columns
		if col.IsIdentity == "1" && col.Extra == "auto_increment" {
			continue
		}

		if val, exists := data[col.FieldName]; exists {
			columnNames = append(columnNames, col.FieldName)
			placeholders = append(placeholders, ":"+col.FieldName)
			processDataBeforeSave(data, col)
			params[col.FieldName] = val
		}
	}

	// Build and prepare query
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) RETURNING id",
		tableName,
		strings.Join(columnNames, ", "),
		strings.Join(placeholders, ", "),
	)

	stmt, err := e.DB.PrepareNamed(query)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Execute query
	if err := stmt.Get(&lastID, params); err != nil {
		return 0, fmt.Errorf("failed to execute insert: %w", err)
	}

	return lastID, nil
}

func (e *SqlxDatabase) UpdateObject(tableName string, object interface{}, tx *sqlx.Tx) error {
	// Convert object to map
	jsonData, err := json.Marshal(object)
	if err != nil {
		return fmt.Errorf("failed to marshal object: %w", err)
	}

	data := make(map[string]interface{})
	if err := json.Unmarshal(jsonData, &data); err != nil {
		return fmt.Errorf("failed to unmarshal to map: %w", err)
	}

	// Get table columns info
	columns, err := e.getTableColumns(tableName)
	if err != nil {
		return fmt.Errorf("failed to get table columns: %w", err)
	}

	// Build update query
	var (
		updates    []string
		params     = make(map[string]interface{})
		primaryKey string
	)

	for _, col := range columns {
		if col.IsIdentity == "1" {
			primaryKey = col.FieldName
			continue
		}

		value, exists := data[col.FieldName]
		if !exists || value == nil {
			continue
		}

		updates = append(updates, fmt.Sprintf("%s = :%s", col.FieldName, col.FieldName))
		processDataBeforeSave(data, col)
		params[col.FieldName] = data[col.FieldName]
	}

	if primaryKey == "" {
		return fmt.Errorf("no primary key found for table %s", tableName)
	}

	// Add primary key to params
	pkValue, exists := data[primaryKey]
	if !exists {
		return fmt.Errorf("primary key value not found in object")
	}
	params[primaryKey] = pkValue

	// Execute update
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s = :%s",
		tableName,
		strings.Join(updates, ", "),
		primaryKey,
		primaryKey,
	)

	if _, err := e.DB.NamedExec(query, params); err != nil {
		return fmt.Errorf("failed to execute update: %w", err)
	}

	return nil
}

func (e *SqlxDatabase) InsertStmt(table string, data map[string]interface{}) error {
	// Example:
	// Input:
	//   table: "users(id,name,email)"
	//   data: map[string]interface{}{
	//     "id": 1,
	//     "name": "John",
	//     "email": "john@example.com"
	//   }
	// Output:
	//   Query: "INSERT INTO users VALUES ($1, $2, $3)"
	//   Args: [1, "John", "john@example.com"]

	// Use strings.LastIndex to avoid multiple splits
	startIdx := strings.LastIndex(table, "(")
	endIdx := strings.LastIndex(table, ")")
	if startIdx == -1 || endIdx == -1 {
		return fmt.Errorf("invalid table format")
	}

	// Get values between parentheses
	values := strings.Split(table[startIdx+1:endIdx], ",")
	n_values := len(values)

	// Pre-allocate string builder with estimated size
	var sb strings.Builder
	sb.Grow(n_values*4 + 2) // Estimate size: n_values * ($ + digit + comma + space) + parentheses

	sb.WriteByte('(')
	for j := 1; j <= n_values; j++ {
		sb.WriteByte('$')
		sb.WriteString(strconv.Itoa(j))
		if j < n_values {
			sb.WriteString(", ")
		}
	}
	sb.WriteByte(')')

	query := fmt.Sprintf("INSERT INTO %s VALUES %s", table[:startIdx], sb.String())

	// Extract values in order
	args := make([]interface{}, n_values)
	for i, col := range values {
		col = strings.TrimSpace(col)
		val, ok := data[col]
		if !ok {
			return fmt.Errorf("missing value for column %s", col)
		}
		args[i] = val
	}

	stmt, err := e.DB.Prepare(query)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	if _, err := stmt.Exec(args...); err != nil {
		return fmt.Errorf("failed to execute insert: %w", err)
	}

	return nil
}

func (e *SqlxDatabase) MultiInsertStmt(table string, data []map[string]interface{}) error {
	/*
		Example:
		table := "users(id,name,email)"
		data := []map[string]interface{}{
			{
				"id": 1,
				"name": "John",
				"email": "john@example.com",
			},
			{
				"id": 2,
				"name": "Jane",
				"email": "jane@example.com",
			},
		}

		Will generate:
		"INSERT INTO users VALUES ($1, $2, $3), ($4, $5, $6)"
		With args: [1, "John", "john@example.com", 2, "Jane", "jane@example.com"]
	*/

	if len(data) == 0 {
		return nil
	}
	// Use strings.LastIndex to avoid multiple splits
	startIdx := strings.LastIndex(table, "(")
	endIdx := strings.LastIndex(table, ")")
	if startIdx == -1 || endIdx == -1 {
		return fmt.Errorf("invalid table format")
	}

	// Get values between parentheses
	values := strings.Split(table[startIdx+1:endIdx], ",")
	n_values := len(values)

	// Pre-allocate string builder with estimated size
	var sb strings.Builder
	n_rows := len(data)
	sb.Grow(len(table) + 7 + n_rows*(n_values*4+3))

	// Write table name
	sb.WriteString("INSERT INTO ")
	sb.WriteString(table[:startIdx])
	sb.WriteString(" VALUES ")

	// Write placeholder values
	args := make([]interface{}, 0, n_rows*n_values)
	x := 1
	for i := 0; i < n_rows; i++ {
		sb.WriteByte('(')
		for j, col := range values {
			col = strings.TrimSpace(col)
			val, ok := data[i][col]
			if !ok {
				return fmt.Errorf("missing value for column %s in row %d", col, i)
			}
			args = append(args, val)

			sb.WriteByte('$')
			sb.WriteString(strconv.Itoa(x))
			if j < n_values-1 {
				sb.WriteString(", ")
			}
			x++
		}
		if i < n_rows-1 {
			sb.WriteString("), ")
		} else {
			sb.WriteByte(')')
		}
	}

	stmt, err := e.DB.Prepare(sb.String())
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	if _, err := stmt.Exec(args...); err != nil {
		return fmt.Errorf("failed to execute multi-insert: %w", err)
	}

	return nil
}

// `values` must be castable to string.
func (e *SqlxDatabase) SelectInStmt(table string, col string, values []string) string {
	if len(values) == 0 {
		return ""
	}

	// Pre-allocate string builder with estimated size
	var sb strings.Builder
	// Estimate size: table + col + values length + extra chars
	sb.Grow(len(table)*2 + len(col) + len(values)*4)

	// Build VALUES clause efficiently
	sb.WriteString("SELECT ")
	sb.WriteString(table)
	sb.WriteString(".* FROM ")
	sb.WriteString(table)
	sb.WriteString(" INNER JOIN (VALUES ")

	for i, value := range values {
		sb.WriteByte('(')
		sb.WriteByte('\'')
		sb.WriteString(value)
		sb.WriteByte('\'')
		sb.WriteByte(')')
		if i < len(values)-1 {
			sb.WriteString(", ")
		}
	}

	sb.WriteString(") values(v) ON ")
	sb.WriteString(col)
	sb.WriteString(" = v")

	return sb.String()
}

func (e *SqlxDatabase) Select(query string, conds map[string]interface{}) ([]map[string]interface{}, error) {
	// Example:
	// query := "SELECT * FROM users WHERE age > :min_age"
	// conds := map[string]interface{}{
	//     "min_age": 18,
	// }
	// results, err := db.Select(query, conds)
	rows, err := e.DB.NamedQuery(query, conds)
	if err != nil {
		return nil, err
	}
	return selectScan(rows)
}

func selectScan(rows *sqlx.Rows) ([]map[string]interface{}, error) {
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Pre-allocate memory for results
	numColumns := len(columns)
	values := make([]interface{}, numColumns)
	for i := range values {
		values[i] = new(interface{})
	}

	results := make([]map[string]interface{}, 0)

	// Scan rows
	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		// Create map for current row
		dest := make(map[string]interface{}, numColumns)
		for i, column := range columns {
			val := *(values[i].(*interface{}))

			// Convert []uint8 to string for better usability
			if byteSlice, ok := val.([]uint8); ok {
				dest[column] = string(byteSlice)
			} else {
				dest[column] = val
			}
		}
		results = append(results, dest)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return results, nil
}

func processDataBeforeSave(data map[string]interface{}, column *Columns) {
	// Get the value for the field
	value, exists := data[column.FieldName]
	if !exists {
		return
	}

	// Handle nil values
	if value == nil {
		if column.IsNullable != "YES" {
			// Set default value for non-nullable fields
			switch column.DataType {
			case "varchar":
				data[column.FieldName] = ""
			case "int", "bigint":
				data[column.FieldName] = 0
			case "boolean":
				data[column.FieldName] = false
			}
		}
		return
	}

	switch column.DataType {
	case "varchar":
		// Handle string type
		strVal, ok := value.(string)
		if !ok {
			// Try to convert to string if possible
			data[column.FieldName] = fmt.Sprintf("%v", value)
			return
		}

		// Truncate if exceeds max length
		if column.MaxLength > 0 && len(strVal) > column.MaxLength {
			data[column.FieldName] = strVal[:column.MaxLength]
		}

	case "int", "bigint":
		// Handle numeric types
		switch v := value.(type) {
		case float64:
			data[column.FieldName] = int64(v)
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				data[column.FieldName] = i
			}
		}

	case "boolean":
		// Handle boolean type
		switch v := value.(type) {
		case string:
			if b, err := strconv.ParseBool(v); err == nil {
				data[column.FieldName] = b
			}
		case int:
			data[column.FieldName] = v != 0
		}
	}
}

func (e *SqlxDatabase) getTableColumns(tableName string) ([]*Columns, error) {
	listColumn := columnDB[tableName]
	if listColumn == nil {
		queryColumn := `
			SELECT
				d.column_name AS FieldName,
				d.udt_name AS DataType,
				COALESCE(d.character_maximum_length, d.numeric_precision, d.datetime_precision, 0) AS MaxLength,
				CASE WHEN t.conname IS NOT NULL THEN '1' ELSE '' END AS IsIdentity,
				CASE WHEN s.extra IS NOT NULL THEN 'auto_increment' ELSE '' END Extra,
				d.is_nullable AS IsNullable
			FROM information_schema.columns d
			JOIN pg_class c ON c.relname = d.table_name
			JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND d.column_name = a.attname
			LEFT JOIN pg_constraint t ON (a.attrelid = t.conrelid AND t.contype = 'p' AND a.attnum = t.conkey[1])
			LEFT JOIN pg_attrdef f ON (a.attrelid = f.adrelid AND a.attnum = f.adnum)
			LEFT JOIN (
				SELECT 'nextval(''' || c.relname || '''::regclass)' AS extra
				FROM pg_class c WHERE c.relkind = 'S'
			) s ON pg_get_expr(f.adbin, f.adrelid) = s.extra
			WHERE c.relname = $1
			ORDER BY a.attnum`

		listColumn = make([]*Columns, 0)
		if err := e.DB.Select(&listColumn, queryColumn, tableName); err != nil {
			return nil, err
		}
		columnDB[tableName] = listColumn
	}
	return listColumn, nil
}

// BuildSQLJoinValues accepts a slice of structs or primitives.
// If fieldNames is empty → treat slice as primitive (e.g. []string).
// If fieldNames is not empty → treat slice as []struct with those fields.
//
// Returns: VALUES string and flattened args slice. startAt is the current placeholder index. could use in next query.
func (e *SqlxDatabase) BuildSQLJoinValues(slice any, fieldNames []string, startAt *int) (string, []any, error) {
	v := reflect.ValueOf(slice)
	if v.Kind() != reflect.Slice {
		return "", nil, fmt.Errorf("input must be a slice")
	}
	if v.Len() == 0 {
		return "", nil, fmt.Errorf("empty slice not allowed")
	}

	var (
		placeholders []string
		args         []any
	)

	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i)
		if elem.Kind() == reflect.Ptr {
			elem = elem.Elem()
		}

		if len(fieldNames) == 0 {
			args = append(args, elem.Interface())
			placeholders = append(placeholders, fmt.Sprintf("($%d)", *startAt))
			*startAt++
			continue
		}

		if elem.Kind() != reflect.Struct {
			return "", nil, fmt.Errorf("expected struct elements for fieldNames")
		}

		var rowPlaceholders []string
		for _, field := range fieldNames {
			f := elem.FieldByName(field)
			if !f.IsValid() {
				return "", nil, fmt.Errorf("field %s not found in struct", field)
			}
			args = append(args, f.Interface())
			rowPlaceholders = append(rowPlaceholders, fmt.Sprintf("$%d", *startAt))
			*startAt++
		}
		placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", ")))
	}

	return strings.Join(placeholders, ", "), args, nil
}

func (e *SqlxDatabase) DoInTransaction(
	ctx context.Context,
	fn func(*sqlx.Tx) (any, error),
	isolation sql.IsolationLevel,
) (any, error) {
	var (
		tx  *sqlx.Tx
		err error
	)

	switch isolation {
	case sql.LevelDefault:
	case sql.LevelReadCommitted:
		tx, err = e.BeginReadCommitted(ctx)

	case sql.LevelRepeatableRead:
		tx, err = e.BeginRepeatableRead(ctx)

	case sql.LevelSerializable:
		tx, err = e.BeginSerializable(ctx)

	default:
		return nil, fmt.Errorf("unsupported transaction isolation level: %v", isolation)
	}

	if err != nil {
		return nil, err
	}

	defer func() {
		if r := recover(); r != nil {
			_ = e.RollBack(tx)
			panic(r)
		}

		_ = e.RollBack(tx)
	}()

	result, err := fn(tx)
	if err != nil {
		return nil, err
	}

	if err := e.Commit(tx); err != nil {
		return nil, err
	}

	return result, nil
}
