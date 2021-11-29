package table

import (
	"csv-stuff/data"
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
	Definition data.Definition ///e.g. time string
}

type ImportCfg struct{
	StaticColData []NamedOffset
	StaticRowData NamedOffset
	DataStart NamedOffset
}

func NewImportTable(reader Reader, cfg ImportCfg)*ImportTable {
	return &ImportTable{
		reader: reader,
		importcfg: cfg,
	}
}

func (p *ImportTable)Import( store data.Store)error{

	fields := make([]data.Definition, 0)
	fields = append(fields, data.Definition(p.importcfg.StaticRowData.Definition))
	for _, coldata :=range p.importcfg.StaticColData{
		fields = append(fields, data.Definition(coldata.Definition))
	}
	fields = append(fields, data.Definition(p.importcfg.DataStart.Definition))
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

