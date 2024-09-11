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
	// The function is called after the change has been applied to raft successfully.
	//
	// Returning an error from this function will fail the `Store` process. In such a
	// case the Cache.Store function will return the uuid of the successful cache update
	// AND the error of the failed persistence returned here. The is no rollback of
	// the cache update. The possible discrepancy needs to be handled by client code.
	Store(data []byte) error
}
