package data

import (
	"errors"
	"log"
)

type DataRow struct{
	fieldindexes map[string]int
	data []interface{}
}

func NewDataRow(vals []interface{}, fields map[string]int)*DataRow{
	if len(vals) != len(fields){
		log.Panic("Invalid data row, data len must match the field map ",
			len(vals), " != ", len(fields))
	}
	return &DataRow{
		fieldindexes: fields,
		data: vals,
	}
}

func (p *DataRow)Get(fieldname string)(val interface{}, err error){
	//fmt.Println("Looking for field val ", fieldname)
	cmpindex, exists := p.fieldindexes[fieldname]
	if !exists{
		return nil, errors.New("Invalid field name " + fieldname)
	}
	cmpval := p.data[cmpindex]
	//fmt.Println("Found field ", fieldname, " with val ", cmpval)
	return cmpval, nil
}

func (p *DataRow)Set(fieldname string, val interface{})error{
	//fmt.Println("Looking for field val ", fieldname)
	cmpindex, exists := p.fieldindexes[fieldname]
	if !exists{
		return errors.New("Invalid field name " + fieldname)
	}
	p.data[cmpindex] = val
	//fmt.Println("Found field ", fieldname, " with val ", cmpval)
	return nil
}

func (p *DataRow)GetRow()[]interface{}{
	return p.data
}

func (p *DataRow)SetRow(vals []interface{}){
	if len(vals) != len(p.data){
		log.Panic("Invalid operation: cannot set a row of different langth to original")
	}
	p.data = vals
}
