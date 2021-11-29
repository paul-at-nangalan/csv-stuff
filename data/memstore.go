package data

import "strings"

type FieldType string
type Definition string

func (p Definition)GetName()string{
	name := strings.Split(string(p), " [\t]")[0]
	return name
}

const(
	FLOAT FieldType = "double"
	STRING FieldType = "string"
	INT FieldType = "int"
	TIMESTAMP = "timestamp"
)

type Field struct{
	name string
	fieldtype FieldType
	format string
}

type Store interface {
	Create(fields []Definition)error
	AddRow()error
	AddDataToCurrentRow(data interface{}, fieldname string)error
	Query(colnames []string, atrow int64)([]interface{}, error)
}

type InvalidDefinition struct{
	reason string
}

func (p *InvalidDefinition) Error() string {
	return p.reason
}

type InvalidFieldName struct{
	reason string
}

func (p *InvalidFieldName) Error() string{
	return p.reason
}

/**
Store data in memory
Order is not guaranteed, but should match the order the data is added
 */
type MemStore struct{
	fields []Field
	fieldindx map[string]int
	data [][]interface{}
}

func NewMemStore()Store{
	return &MemStore{}
}

func (p *MemStore)Create(defs []Definition)error{
	p.fields = make([]Field, len(defs))
	p.fieldindx = make(map[string]int)
	p.data = make([][]interface{},0)
	for i, def := range defs{
		defparts := strings.Split(string(def), "\t ")
		if len(defparts) < 2{
			return &InvalidDefinition{
				reason: "Definition should be NAME TYPE, e.g. price double",
			}
		}
		switch FieldType(defparts[1]){
		case FLOAT:
			p.fields[i].fieldtype = FLOAT
		case STRING:
			p.fields[i].fieldtype = STRING
		case INT:
			p.fields[i].fieldtype = INT
		case TIMESTAMP:
			p.fields[i].fieldtype = TIMESTAMP
			p.fields[i].format = defparts[2]
		default:
			return &InvalidDefinition{
				reason: "Invalid field type: " + defparts[1],
			}
		}
		p.fields[i].name = defparts[0]
		p.fieldindx[defparts[0]] = i
	}
	return nil
}

func (p *MemStore)AddDataToCurrentRow(data interface{}, fieldname string)error{
	indx, ok := p.fieldindx[fieldname]
	if !ok{
		return &InvalidFieldName{
			reason: "Unrecognised field name " + fieldname,
		}
	}
	p.data[len(p.data) - 1][indx] = data
	return nil
}

func (p *MemStore)AddRow()error{
	newrow := make([]interface{}, len(p.fields))
	p.data = append(p.data, newrow)
	return nil
}

func (p *MemStore)Query(colnames []string, atrow int64)([]interface{}, error){
	result := make([]interface{}, len(colnames))
	if atrow > int64(len(p.data)){
		return nil, nil
	}

	for i, colname := range colnames{
		colindx, ok := p.fieldindx[colname]
		if !ok{
			return nil, &InvalidFieldName{
				reason: "Invalid field name " + colname,
			}
		}
		val := p.data[atrow][colindx]
		result[i] = val
	}
	return result, nil
}
