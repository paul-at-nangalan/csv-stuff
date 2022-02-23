package schema

import (
	"strings"
)


type Field interface {
	Name()string
}

type Store interface {
	Create(fields []Definition) error
	AddRow() error
	AddDataToCurrentRow(data interface{}, fieldname string) error
	Query(colnames []string, atrow int64) ([]interface{}, error)
	ListErrors() []error
	GetFields()[]Field
	GetRow(rowindx int64)(datarow DataRow, valid bool)
}

type FieldType string

type Definition string

func (p Definition) GetName() string {
	name := strings.Fields(string(p))[0]
	return name
}

type DataRow interface {
	Get(fieldname string) (val interface{}, err error)
	Set(fieldname string, val interface{}) error
	GetRow() []interface{}
	SetRow(vals []interface{})
}
