package store

func NewStableInMemory() *StableInMemory {
	return &StableInMemory{}
}

type StableInMemory struct{}

func (s *StableInMemory) Set(key []byte, val []byte) error {
	// TODO implement me
	panic("implement me")
}

func (s *StableInMemory) Get(key []byte) ([]byte, error) {
	// TODO implement me
	panic("implement me")
}

func (s *StableInMemory) SetUint64(key []byte, val uint64) error {
	// TODO implement me
	panic("implement me")
}

func (s *StableInMemory) GetUint64(key []byte) (uint64, error) {
	// TODO implement me
	panic("implement me")
}
