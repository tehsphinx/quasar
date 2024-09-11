// Copyright (c) RealTyme SA. All rights reserved.

package stores

// PersistentStorage defines an interface that can be used to update e.g. a database
// when cache data is updated. It is not meant to persist the cache itself. For this
// the underlying raft implementation already provides interfaces to be implemented.
type PersistentStorage interface {
	// Store will be called for every Store command from the master node only.
	// This is meant to be used when updating data in the cache that should also
	// be updated in a database.
	//
	// The function is called before the change is applied to the cache via raft.
	//
	// Returning an error from this function will fail the `Store` process. If the
	// update of the cache fails after the storage was updated, it is up to the client
	// code to deal with the discrepancy between persistent store and cache.
	Store(data StoreData) error
}

// StoreData defines an interface used in the Store function of the PersistentStorage interface.
type StoreData interface {
	// Data returns the data to be stored.
	Data() []byte
	// UpdateCommand allows the set a new command to update the cache with. This can be
	// helpful e.g. in case of a partial update of a data structure. The partial update
	// can be applied to the persistent storage and turned into a full update command
	// for the cache. This can help to make sure there is no drift between the persistent
	// storage and the cache.
	UpdateCommand(data []byte)
}

// PersistData defines an interface used internally to be able to check if data was updated.
type PersistData interface {
	StoreData

	IsUpdated() bool
}

func NewPersistData(data []byte) PersistData {
	return &persistData{data: data}
}

type persistData struct {
	data    []byte
	updated bool
}

func (s *persistData) Data() []byte {
	return s.data
}

func (s *persistData) UpdateCommand(data []byte) {
	s.data = data
	s.updated = true
}

func (s *persistData) IsUpdated() bool {
	return s.updated
}
