package data

import (
	"fmt"
	"github.com/paul-at-nangalan/csv-stuff/schema"
	"strings"
)

const(
	FLOAT     schema.FieldType = "double"
	STRING    schema.FieldType = "string"
	INT       schema.FieldType = "int"
	TIMESTAMP                  = "timestamp"
)

type Field struct{
	name      string
	fieldtype schema.FieldType
	format    string
}

func (p Field)Name()string{
	return p.name
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

	errors []error
}

func NewMemStore() schema.Store {
	return &MemStore{}
}

func (p *MemStore)ListErrors()[]error{
	panic("Not yet implemented")
	return nil
}

func (p *MemStore)Create(defs []schema.Definition)error{
	p.fields = make([]Field, len(defs))
	p.fieldindx = make(map[string]int)
	p.data = make([][]interface{},0)
	p.errors = make([]error, 0)
	for i, def := range defs{
		fmt.Println("Defs: ", def)
		defparts := strings.Fields(string(def))
		//fmt.Println("Len def parts ", len(defparts))
		if len(defparts) < 2{
			return &InvalidDefinition{
				reason: "Definition should be NAME TYPE, e.g. price double " + string(def),
			}
		}
		typepart := defparts[len(defparts) - 1]
		fmt.Println("Type ", typepart)
		switch schema.FieldType(typepart){
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
		p.fields[i].name = strings.TrimSpace(strings.TrimSuffix(string(def), typepart))
		fmt.Println("Filed at ", i, " is ", p.fields[i].Name())
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
	if atrow >= int64(len(p.data)){
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

func (p *MemStore)GetRow(rowindx int64)(datarow schema.DataRow, valid bool){
	if rowindx >= int64(len(p.data)){
		return nil, false
	}
	datarow = &DataRow{
		fieldindexes: p.fieldindx,
		data: p.data[rowindx],
	}
	return datarow, true
}

func (p *MemStore)GetFields()[]schema.Field{
	fields := make([]schema.Field, len(p.fields))
	for i, field := range p.fields{
		fields[i] = field
	}
	return fields
}
