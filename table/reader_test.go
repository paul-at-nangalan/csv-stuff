package table

import (
	"github.com/paul-at-nangalan/csv-stuff/data"
	"testing"
)

func TestImportTable_Import(t *testing.T) {
	testdata := [][]string{
		{"","","","New York", "Dublin", "Hong Kong"},
		{"","","","", "", ""},
		{"A","","","3000", "4500", "643.234"},
		{"B","","","3001", "4501", "644.234"},
		{"C","","","3002", "4502", "645.234"},
		{"D","","","3003", "4503", "646.234"},
	}
	expectcols := []string{"id", "city", "value"}
	expect := [][]string{
		{"A", "New York", "3000"},
		{"A", "Dublin", "4500"},
		{"A", "Hong Kong", "643.234"},
		{"B", "New York", "3001"},
		{"B", "Dublin", "4501"},
		{"B", "Hong Kong", "644.234"},
		{"C", "New York", "3002"},
		{"C", "Dublin", "4502"},
		{"C", "Hong Kong", "645.234"},
		{"D", "New York", "3003"},
		{"D", "Dublin", "4503"},
		{"D", "Hong Kong", "646.234"},

	}
	mockreader := &MockReader{
		data: testdata,
	}
	importcfg := ImportCfg{}
	importcfg.StaticRowData.Vertical = 3
	importcfg.StaticRowData.Horizontal = 0
	importcfg.StaticRowData.Definition = "city string"

	importcfg.StaticColData = make([]NamedOffset, 1)
	importcfg.StaticColData[0].Vertical = 0
	importcfg.StaticColData[0].Horizontal = 2
	importcfg.StaticColData[0].Definition = "id string"

	importcfg.DataStart.Vertical = 3
	importcfg.DataStart.Horizontal = 2
	importcfg.DataStart.Definition = "value int"

	importer := NewImportTable(mockreader, importcfg)
	store := data.NewMemStore()

	err := importer.Import(store)
	if err != nil {
		t.Error("Failed to import data with error ", err)
		t.FailNow()
	}

	for i := 0; ; i++ {
		res, err := store.Query(expectcols, int64(i))
		if err != nil{
			t.Error("Failed to query data at row ", i)
			t.FailNow()
		}
		if res == nil{
			if i < len(expect){
				t.Error("Failed to read all data ", i)
				t.FailNow()
			}
			break
		}

		if expect[i][0] != res[0].(string){
			t.Error("Unexpected value at ", expectcols[0], expect[i][0], res[0], " at row ", i)
		}
		if expect[i][1] != res[1].(string){
			t.Error("Unexpected value at ", expectcols[1], expect[i][1], res[1], " at row ", i)
		}
		if expect[i][2] != res[2].(string){
			t.Error("Unexpected value at ", expectcols[2], expect[i][2], res[2], " at row ", i)
		}
	}
}
