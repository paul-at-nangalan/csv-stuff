package list

import (
	"fmt"
	"github.com/paul-at-nangalan/csv-stuff/schema"
)

type Writer interface {
	Write([]string)error
}
type Exporter struct{
	writer Writer
}

func NewExporter(writer Writer)*Exporter{
	return &Exporter{
		writer: writer,
	}
}

func (p* Exporter)Export(store schema.Store, coldefs []schema.Definition)error{
	colnames := make([]string, len(coldefs))
	for i, col := range coldefs{
		colnames[i] = col.GetName()
	}
	err := p.writer.Write(colnames)
	if err != nil{
		return err
	}
	for i := 0; ; i++{
		resp, err := store.Query(colnames, int64(i))
		if err != nil{
			return err
		}
		if resp == nil{
			break ///end of data
		}
		output := make([]string, len(resp))
		for x := 0; x < len(output); x++{
			output[x] = fmt.Sprint(resp[x])
		}
		p.writer.Write(output)
	}
	return nil
}
