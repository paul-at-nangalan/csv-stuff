package csv_stuff

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/paul-at-nangalan/csv-stuff/data"
	"github.com/paul-at-nangalan/csv-stuff/list"
	"github.com/paul-at-nangalan/csv-stuff/table"
	"github.com/paul-at-nangalan/json-config/cfg"
	"os"
)

func main(){

	cfgdir := ""
	importfile := ""
	exportfile := ""

	flag.StringVar(&cfgdir, "cfg", "./cfg", "Config directory")
	flag.StringVar(&importfile, "infile", "", "File to import from")
	flag.StringVar(&exportfile, "outfile", "", "File to export to")
	flag.Parse()

	cfg.Setup(cfgdir)

	importcfg := table.ImportCfg{}
	err := cfg.Read("import", &importcfg)
	if err != nil{
		fmt.Println("Failed to read config ", err)
		return
	}
	store := data.NewMemStore()
	defs := make([]data.Definition, 0)
	defs = append(defs, importcfg.StaticRowData.Definition)
	defs = append(defs, importcfg.DataStart.Definition)
	for _, def := range importcfg.StaticColData{
		defs = append(defs, def.Definition)
	}
	err = store.Create(defs)
	if err != nil{
		fmt.Println("Failed to create mem store ", err)
		return
	}
	infile, err := os.Open(importfile)
	if err != nil{
		fmt.Println("Failed to open import file ", importfile, err)
		return
	}
	defer infile.Close()
	reader := csv.NewReader(infile)
	importer := table.NewImportTable(reader, importcfg)
	err = importer.Import(store)
	if err != nil{
		fmt.Println("Failed to import data ", err)
		return
	}

	outfile, err := os.Create(exportfile)
	if err != nil{
		fmt.Println("Failed to create output file ", exportfile, err)
		return
	}
	defer outfile.Close()
	writer := csv.NewWriter(outfile)
	exporter := list.NewExporter(writer)
	err = exporter.Export(store, defs)
	if err != nil{
		fmt.Println("Failed to export data: ", err)
		return
	}
}
