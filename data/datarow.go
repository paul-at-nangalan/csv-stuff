package data

import "errors"

type DataRow struct{
	fieldindexes map[string]int
	data []interface{}
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
