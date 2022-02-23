package list

import (
	"github.com/paul-at-nangalan/csv-stuff/data"
	"testing"
)

func Test_ReadCsv(t *testing.T) {
	mockreader := NewMockReader()
	mockreader.AddRow([]string{"", "", "", "", "", ""})
	mockreader.AddRow([]string{"", "", "", "", "", ""})
	mockreader.AddRow([]string{"fruit", "date", "price", "colour", "height", "weight"})
	mockreader.AddRow([]string{"orange", "2022-01-02", "2.13", "orange", "0.09", "1.1"})
	mockreader.AddRow([]string{"apple", "2022-02-11", "3.443", "green", "0.10", "1.2"})
	mockreader.AddRow([]string{"bannana", "2021-11-12", "34.2", "yellow", "1.25", "2.3"})
	mockreader.AddRow([]string{"durian", "2020-05-19", "69", "cream", "0.59", "5.23"})
	mockreader.AddRow([]string{"melon", "2030-10-12", "75.233", "green", "0.23", "9"})
	cfg := Config{
		HeaderRowIndex: 2,
	}
	importer := NewCsvImporter(cfg, mockreader)

	datastore := data.NewMemStore()
	err := importer.Import(datastore)
	if err != nil {
		t.Error("Unexpected error ", err)
	}
	rowoffset := 2
	i := 0
	fields := datastore.GetFields()
	testfields := mockreader.data[i + rowoffset]
	i++
	for i, field := range fields{
		if testfields[i] != field.Name(){
			t.Error("Mismatch field name, expected ", testfields[i], " got ", field.Name())
		}
	}
	for ; i + rowoffset < len(mockreader.data); i++ {
		row, valid := datastore.GetRow(int64(i - 1))
		if !valid{
			t.Error("Get row within range returns not valid, index ", i)
		}
		rawrow := row.GetRow()
		testrow := mockreader.data[i + rowoffset]
		for x, item := range testrow{
			if item != rawrow[x].(string){
				t.Error("Mismatch value at row ", i, " col ", x,
					" expected ", item, " got ", rawrow[x])
			}
		}
	}
	_, valid := datastore.GetRow(int64(i))
	if valid{
		t.Error("Get row outside range returns valid (expected invalid (false), index ", i)
	}
}
