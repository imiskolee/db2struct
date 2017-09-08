package db2struct

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// GetColumnsFromMysqlTable Select column details from information schema and return map of map
func GetColumnsFromMysqlTable(mariadbUser string, mariadbPassword string, mariadbHost string, mariadbPort int, mariadbDatabase string, mariadbTable string) (*map[string]map[string]string, error) {

	var err error
	var db *sql.DB
	if mariadbPassword != "" {
		db, err = sql.Open("mysql", mariadbUser+":"+mariadbPassword+"@tcp("+mariadbHost+":"+strconv.Itoa(mariadbPort)+")/"+mariadbDatabase+"?&parseTime=True")
	} else {
		db, err = sql.Open("mysql", mariadbUser+"@tcp("+mariadbHost+":"+strconv.Itoa(mariadbPort)+")/"+mariadbDatabase+"?&parseTime=True")
	}
	defer db.Close()

	// Check for error in db, note this does not check connectivity but does check uri
	if err != nil {
		fmt.Println("Error opening mysql db: " + err.Error())
		return nil, err
	}

	// Store colum as map of maps
	columnDataTypes := make(map[string]map[string]string)
	// Select columnd data from INFORMATION_SCHEMA
	columnDataTypeQuery := "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND table_name = ?"

	if Debug {
		fmt.Println("running: " + columnDataTypeQuery)
	}

	rows, err := db.Query(columnDataTypeQuery, mariadbDatabase, mariadbTable)

	if err != nil {
		fmt.Println("Error selecting from db: " + err.Error())
		return nil, err
	}
	if rows != nil {
		defer rows.Close()
	} else {
		return nil, errors.New("No results returned for table")
	}

	for rows.Next() {
		var column string
		var dataType string
		var nullable string
		rows.Scan(&column, &dataType, &nullable)

		columnDataTypes[column] = map[string]string{"value": dataType, "nullable": nullable}
	}

	return &columnDataTypes, err
}

// Generate go struct entries for a map[string]interface{} structure
func generateMysqlTypes(obj map[string]map[string]string, depth int, jsonAnnotation bool, gormAnnotation bool, gureguTypes bool, structType string) (string, string, string) {
	structure := "struct {"

	from := ""
	to := ""
	keys := make([]string, 0, len(obj))
	for key := range obj {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		mysqlType := obj[key]
		nullable := false
		if mysqlType["nullable"] == "YES" {
			nullable = true
		}

		// Get the corresponding go value type for this mysql type
		var valueType string
		// If the guregu (https://github.com/guregu/null) CLI option is passed use its types, otherwise use go's sql.NullX

		valueType = mysqlTypeToGoType(mysqlType["value"], nullable, gureguTypes, structType)
		realType := mysqlRealType(mysqlType["value"], nullable, gureguTypes, structType)

		fieldName := fmtFieldName(stringifyFirstChar(key))
		var annotations []string
		if gormAnnotation == true {
			annotations = append(annotations, fmt.Sprintf("gorm:\"column:%s\"", key))
		}
		if jsonAnnotation == true {
			annotations = append(annotations, fmt.Sprintf("json:\"%s\"", key))
		}
		if len(annotations) > 0 {
			structure += fmt.Sprintf("\n%s %s `%s`",
				fieldName,
				valueType,
				strings.Join(annotations, " "))

		} else {
			structure += fmt.Sprintf("\n%s %s",
				fieldName,
				valueType)
		}
		fname := "s." + fieldName
		tname := "t." + fieldName
		fval := fname
		tval := tname
		if structType == PrefixModel {
			switch valueType {
			case "sql.NullInt64", "sql.NullInt", "sql.NullFloat64", "sql.NullString":
				reg, _ := regexp.Compile("Null(\\w+)")
				t := reg.FindStringSubmatch(valueType)
				zeroValue := "0"
				if t[1] == "Int" || t[1] == "Int64" {
					zeroValue = "0"
				} else if t[1] == "Float64" {
					zeroValue = "0.0"
				} else {
					zeroValue = "\"\""
				}

				if realType == sqlNullInt {
					fval = "int(" + (fname) + "." + (t[1]) + ")"
					tval = "int64(" + (tname) + ")"
				} else {
					fval = (fname) + "." + (t[1])
					tval = tname
				}

				to += fmt.Sprintf("if %s.Valid {\n %s=%s \n}\n", fname, tname, fval)
				from += fmt.Sprintf("if %s == %s {\n%s.%s=%s\n%s.Valid=false\n}else{\n%s.%s=%s\n}\n",
					tname, zeroValue, fname, t[1], zeroValue, fname, fname, t[1], tval)
				break
			default:
				from += (fname + "=" + tname + "\n")
				to += (tname + "=" + fname + "\n")
				break
			}
		} else if structType == PrefixLogic {
			switch realType {
			case "sql.NullInt64", "sql.NullInt", "sql.NullFloat64":
				reg, _ := regexp.Compile("Null(\\w+)")
				t := reg.FindStringSubmatch(realType)
				zeroValue := "0"
				tfname := "Int64"
				if t[1] == "Int" || t[1] == "Int64" {
					zeroValue = "0"
				} else if t[1] == "Float64" {
					zeroValue = "0.0"
					tfname = "Float64"
				} else {
					zeroValue = "\"\""
				}
				to += fmt.Sprintf("if %s != %s {\n%s=new(json.Number)\n*%s=json.Number(fmt.Sprint(%s))}\n", fname, zeroValue, tname, tname, fval)
				from += fmt.Sprintf("if %s != nil {\ntemp ,err := (*%s).%s()\nif err != nil {\nreturn err\n}\n%s=%s(temp)\n}\n",
					tname, tname, tfname, fname, valueType)
				break
			case "int", "int64", "float32", "float64":
				zeroValue := "0"
				tfname := "Int64"
				if realType == "float32" || realType == "float64" {
					zeroValue = "0.0"
					tfname = "Float64"
				}
				to += fmt.Sprintf("if %s != %s {\n%s=json.Number(fmt.Sprint(%s))}\n", fname, zeroValue, tname, fval)
				from += fmt.Sprintf("{\ntemp ,err := %s.%s()\nif err != nil {\nreturn err\n}\n%s=%s(temp)\n}\n",
					tname, tfname, fname, valueType)
				break
			case "sql.NullString":
				to += fmt.Sprintf("if %s != \"\" {\n%s=new(%s)\n*%s=%s\n}\n", fname, tname, valueType, tname, fval)
				from += fmt.Sprintf("if %s != nil {\n%s=*%s\n}\n", tname, fname, tname)
				break
			default:
				from += (fname + "=" + tname + "\n")
				to += (tname + "=" + fname + "\n")
				break
			}
		}

	}
	return structure, from, to
}

func mysqlRealType(mysqlType string, nullable bool, gureguTypes bool, structType string) string {
	switch mysqlType {
	case "tinyint", "int", "smallint", "mediumint":
		if nullable {
			if gureguTypes {
				return gureguNullInt
			}
			return sqlNullInt
		}
		return golangInt
	case "bigint":
		if nullable {
			if gureguTypes {
				return gureguNullInt
			}
			return sqlNullInt64
		}
		return golangInt64
	case "char", "enum", "varchar", "longtext", "mediumtext", "text", "tinytext":
		if nullable {
			if gureguTypes {
				return gureguNullString
			}
			return sqlNullString
		}
		return "string"
	case "date", "datetime", "time", "timestamp":
		if nullable && gureguTypes {
			return gureguNullTime
		}
		return golangTime
	case "decimal", "double":
		if nullable {
			if gureguTypes {
				return gureguNullFloat
			}
			return sqlNullFloat
		}
		return golangFloat64
	case "float":
		if nullable {
			if gureguTypes {
				return gureguNullFloat
			}
			return sqlNullFloat
		}
		return golangFloat32
	case "binary", "blob", "longblob", "mediumblob", "varbinary":
		return golangByteArray
	}
	return ""
}

// mysqlTypeToGoType converts the mysql types to go compatible sql.Nullable (https://golang.org/pkg/database/sql/) types
func mysqlTypeToGoType(mysqlType string, nullable bool, gureguTypes bool, structType string) string {
	switch mysqlType {
	case "tinyint", "int", "smallint", "mediumint":
		if nullable {
			if gureguTypes {
				return mapping[structType][gureguNullInt]
			}
			return mapping[structType][sqlNullInt]
		}
		return mapping[structType][golangInt]
	case "bigint":
		if nullable {
			if gureguTypes {
				return mapping[structType][gureguNullInt]
			}
			return mapping[structType][sqlNullInt64]
		}
		return mapping[structType][golangInt64]
	case "char", "enum", "varchar", "longtext", "mediumtext", "text", "tinytext":
		if nullable {
			if gureguTypes {
				return mapping[structType][gureguNullString]
			}
			return mapping[structType][sqlNullString]
		}
		return "string"
	case "date", "datetime", "time", "timestamp":
		if nullable && gureguTypes {
			return mapping[structType][gureguNullTime]
		}
		return mapping[structType][golangTime]
	case "decimal", "double":
		if nullable {
			if gureguTypes {
				return mapping[structType][gureguNullFloat]
			}
			return mapping[structType][sqlNullFloat]
		}
		return mapping[structType][golangFloat64]
	case "float":
		if nullable {
			if gureguTypes {
				return mapping[structType][gureguNullFloat]
			}
			return mapping[structType][sqlNullFloat]
		}
		return mapping[structType][golangFloat32]
	case "binary", "blob", "longblob", "mediumblob", "varbinary":
		return mapping[structType][golangByteArray]
	}
	return ""
}
