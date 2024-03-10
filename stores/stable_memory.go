package stores

func NewStableInMemory() *StableInMemory {
	return &StableInMemory{
		store:       make(map[string][]byte),
		storeUint64: make(map[string]uint64),
	}
}

type StableInMemory struct {
	store       map[string][]byte
	storeUint64 map[string]uint64
}

func (s *StableInMemory) Set(key []byte, val []byte) error {
	s.store[string(key)] = val
	return nil
}

func (s *StableInMemory) Get(key []byte) ([]byte, error) {
	val := s.store[string(key)]
	return val, nil
}

func (s *StableInMemory) SetUint64(key []byte, val uint64) error {
	s.storeUint64[string(key)] = val
	return nil
}

func (s *StableInMemory) GetUint64(key []byte) (uint64, error) {
	val := s.storeUint64[string(key)]
	return val, nil
}
