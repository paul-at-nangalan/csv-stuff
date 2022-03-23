package list

import (
	"github.com/paul-at-nangalan/csv-stuff/schema"
	"io"
	"strings"
)
import "github.com/paul-at-nangalan/errorhandler/handlers"

type Reader interface {
	Read()([]string, error)
}

type Config struct{
	HeaderRowIndex int /// 0 for first row
}

type CsvImporter struct{
	reader Reader
	cfg Config
}

func NewCsvImporter(cfg Config, reader Reader)*CsvImporter{
	return &CsvImporter{
		cfg: cfg,
		reader: reader,
	}
}

func (p *CsvImporter)Import( store schema.Store)error{

	for i := 0; i < p.cfg.HeaderRowIndex; i++{
		_, err := p.reader.Read()
		handlers.PanicOnError(err)
	}
	header, err := p.reader.Read()
	///panic if we get an error before reading any data
	handlers.PanicOnError(err)
	defs := make([]schema.Definition, len(header))
	for i, name := range header{
		if strings.TrimSpace(name) != "" {
			defs[i] = schema.Definition(name + " string") //// we have to treat all as strings
		}
	}
	err = store.Create(defs)
	handlers.PanicOnError(err)

	for i := 0; ; i++{
		row, err := p.reader.Read()
		if err == io.EOF{
			break
		}
		handlers.PanicOnError(err)
		err = store.AddRow()
		handlers.PanicOnError(err)

		for x, val := range row{
			if strings.TrimSpace(header[x]) != "" {
				err = store.AddDataToCurrentRow(val, header[x])
				handlers.PanicOnError(err)
			}
		}
	}
	return nil
}
