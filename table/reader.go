package table

import (
	"github.com/paul-at-nangalan/csv-stuff/schema"
	"io"
)

type Reader interface {
	Read()([]string, error)
}

/**
For importing data in a table format csv
 */
type ImportTable struct {
	reader Reader
	importcfg ImportCfg
	horizontaldata []string
}

type Offset struct {
	Horizontal int
	Vertical   int
}

type NamedOffset struct{
	Offset
	Definition schema.Definition ///e.g. time string
}

type ImportCfg struct{
	StaticColData []NamedOffset
	StaticRowData NamedOffset
	DataStart NamedOffset
}

func (p *ImportCfg) Expand() {
}

func NewImportTable(reader Reader, cfg ImportCfg)*ImportTable {
	return &ImportTable{
		reader: reader,
		importcfg: cfg,
	}
}

func (p *ImportTable)Import( store schema.Store)error{

	fields := make([]schema.Definition, 0)
	fields = append(fields, schema.Definition(p.importcfg.StaticRowData.Definition))
	for _, coldata :=range p.importcfg.StaticColData{
		fields = append(fields, schema.Definition(coldata.Definition))
	}
	fields = append(fields, schema.Definition(p.importcfg.DataStart.Definition))
	//fmt.Println("Creating store with fields ", fields)
	err := store.Create(fields)
	if err != nil{
		return err
	}

	for row := 0; ; row++{
		data, err := p.reader.Read()
		if err == io.EOF{
			return nil
		}
		if err != nil {
			return err
		}

		if row == p.importcfg.StaticRowData.Horizontal{
			p.horizontaldata = make([]string, len(data))
			copy(p.horizontaldata, data)
		}
		if row < p.importcfg.DataStart.Horizontal{
			continue
		}
		////go through the data, col by col, adding a row with the col data and static data
		for col := p.importcfg.DataStart.Vertical; col < len(data); col++{
			store.AddRow()
			for _, coldataref := range p.importcfg.StaticColData{
				store.AddDataToCurrentRow(data[coldataref.Vertical], coldataref.Definition.GetName())
			}
			store.AddDataToCurrentRow(p.horizontaldata[col], p.importcfg.StaticRowData.Definition.GetName())

			store.AddDataToCurrentRow(data[col], p.importcfg.DataStart.Definition.GetName())
		}
	}
}

