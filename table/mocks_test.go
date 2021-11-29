package table

import "io"

type MockReader struct{
	data [][]string
	row int
}

func (p *MockReader) Read() ([]string, error) {
	if p.row >= len(p.data){
		return nil, io.EOF
	}
	row := p.data[p.row]
	p.row++
	return row, nil
}

