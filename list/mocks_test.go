package list

import "io"

type MockReader struct{
	data [][]string
	index int
}

func NewMockReader()*MockReader{
	return &MockReader{
		data: make([][]string, 0),
	}
}

func (p *MockReader)AddRow(row []string){
	p.data = append(p.data, row)
}

func (p *MockReader) Read() ([]string, error) {
	if p.index >= len(p.data){
		return nil, io.EOF
	}
	indx := p.index
	p.index++
	return p.data[indx], nil
}


