package sdfsutils

type matrix struct {
	rowCount int
	colCount int
	data     [][]interface{}
}

func (m *matrix) insert(r, c int, element interface{}) {
	if r < m.rowCount && c < m.colCount {
		m.data[r][c] = element
		return
	}

	for row := 0; row < m.rowCount; row++ {
		for col := m.colCount; col <= c; m.colCount++ {
			m.data[row] = append(m.data[row], WRITE_OP)
		}
	}

	initialSlice := make([]interface{}, 0)
	for i := 0; i < m.colCount; i++ {
		initialSlice = append(initialSlice, WRITE_OP)
	}

	for row := m.rowCount; row <= r; m.rowCount++ {
		m.data = append(m.data, initialSlice)
	}

	m.data[r][c] = element
}

func (m *matrix) get(r, c int) interface{} {
	if r < m.rowCount && c < m.colCount && r >= 0 && c >= 0 {
		return m.data[r][c]
	}
	return nil
}

