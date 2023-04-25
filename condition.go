package boltutil

type Range struct {
	Min, Max []byte
}

func getKey(key any) ([]byte, bool) {
	switch k := key.(type) {
	case []byte:
		return k, true
	case string:
		return []byte(k), true
	default:
		return nil, false
	}
}

func getRange(r any) (*Range, bool) {
	switch rg := r.(type) {
	case Range:
		return &rg, true
	}
}
