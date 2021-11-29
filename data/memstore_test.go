package data

import (
	"reflect"
	"testing"
)

func TestDefinition_GetName(t *testing.T) {
	def := Definition("Abc int")
	name := def.GetName()
	if name != "Abc"{
		t.Error("Failed to get name from def", name)
	}
}

func TestMemStore(t *testing.T) {
	store := NewMemStore()
	def := []Definition{
		"price double",
		"t1 timestamp 2006-01-02",
		"instr string",
	}
	err := store.Create(def)
	if err != nil{
		t.Error("Failed to create mem store ", err)
		t.FailNow()
	}
	rows := []map[string]string{
		{"price": "2.365", "t1": "2021-11-01", "instr": "ABC"},
		{"price": "3422.1", "t1": "2021-11-02", "instr": "BCD"},
		{"price": "228.1992", "t1": "2021-11-03", "instr": "EFG"},
		{"price": "2.95", "t1": "2021-11-03", "instr": "HIJ"},
		{"price": "2.335", "t1": "2021-11-02", "instr": "ABC"},
		{"price": "2.345", "t1": "2021-11-03", "instr": "ABC"},
	}
	for _, row := range rows {
		store.AddRow()
		for key, val := range row{
			store.AddDataToCurrentRow(val, key)
		}
	}

	resp, err := store.Query([]string{"price", "t1"}, 0)
	if err != nil{
		t.Error("Failed to get data at row 0 ", err)
	}
	if resp[0].(string) != "2.365" || resp[1].(string) != "2021-11-01"{
		t.Error("Incorrect data from memstore at 0, got ", resp)
	}

	// {"price": "228.1992", "t1": "2021-11-03", "instr": "EFG"},
	resp, err = store.Query([]string{"price", "instr", "t1"}, 2)
	if err != nil{
		t.Error("Failed to get data at row 2 ", err)
	}
	if resp[0].(string) != "228.1992" || resp[1].(string) != "EFG" || resp[2].(string) != "2021-11-03"{
		t.Error("Incorrect data from memstore at 3, got ", resp)
	}

	//{"price": "2.345", "t1": "2021-11-03", "instr": "ABC"},
	resp, err = store.Query([]string{"price", "instr", "t1"}, 5)
	if err != nil{
		t.Error("Failed to get data at row 5 ", err)
	}
	if resp[0].(string) != "2.345" || resp[1].(string) != "ABC" || resp[2].(string) != "2021-11-03"{
		t.Error("Incorrect data from memstore at 5, got ", resp)
	}

	resp, err = store.Query([]string{"price", "instr", "t1"}, 6)
	if err != nil{
		t.Error("Failed to get data at row 6 ", err)
	}
	if resp != nil{
		t.Error("Got data beyond the end of expected rows")
	}
}

func TestInvalidDefinition_Error(t *testing.T) {

	store := NewMemStore()
	def := []Definition{
		"price float",
		"t1 timestamp 2006-01-02",
		"instr string",
	}
	err := store.Create(def)
	if err == nil{
		t.Error("Failed to create mem store ", err)
		t.FailNow()
	}
	_, ok := err.(*InvalidDefinition)
	if !ok{
		t.Error("Incorrect error for invalid def, got ", err.(reflect.Type).Name())
	}
}