package quasar

import (
	"encoding/json"
	"errors"
	"io"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/pb/v1"
	"github.com/tehsphinx/quasar/stores"
	"google.golang.org/protobuf/proto"
)

func newKeyValueFSM(kv stores.KVStore) *kvFSM {
	return &kvFSM{
		KVStore: kv,
	}
}

var (
	_ FSM        = (*kvFSM)(nil)
	_ logApplier = (*kvFSM)(nil)
)

type kvFSM struct {
	stores.KVStore
	fsm *FSMInjector
}

func (s *kvFSM) Inject(fsm *FSMInjector) {
	s.fsm = fsm
}

func (s *kvFSM) ApplyCmd([]byte) error {
	// should never be called as we implement Apply function.
	return nil
}

func (s *kvFSM) Apply(log *raft.Log) interface{} {
	var cmd pb.Command
	if err := proto.Unmarshal(log.Data, &cmd); err != nil {
		return applyResponse{err: err}
	}

	resp, respErr := s.apply(log, &cmd)

	return applyResponse{
		resp: resp,
		err:  respErr,
	}
}

func (s *kvFSM) apply(log *raft.Log, command *pb.Command) (*pb.CommandResponse, error) {
	switch cmd := command.GetCmd().(type) {
	case *pb.Command_Store:
		return s.store(log.Index, cmd.Store)
	case *pb.Command_ResetCache:
		return nil, s.Reset()
	default:
		// fmt.Printf("%+v\n", command)
		return nil, errors.New("kvFSM.apply: command not implemented")
	}
}

// errKVStoreNoSnapshots is returned when the configured KVStore does not
// implement stores.SnapshotKVStore. Snapshot/Restore/Reset used to panic
// "implement me" — fatal in practice, because raft's default config
// auto-snapshots any FSM that accumulates more than SnapshotThreshold log
// entries (RT-13042 M8).
var errKVStoreNoSnapshots = errors.New("kv store does not implement stores.SnapshotKVStore; snapshots and resets are not supported")

func (s *kvFSM) Snapshot() (raft.FSMSnapshot, error) {
	kv, ok := s.KVStore.(stores.SnapshotKVStore)
	if !ok {
		return nil, errKVStoreNoSnapshots
	}
	items, err := kv.Items()
	if err != nil {
		return nil, err
	}
	bts, err := json.Marshal(items)
	if err != nil {
		return nil, err
	}
	return kvSnapshot(bts), nil
}

func (s *kvFSM) Restore(snapshot io.ReadCloser) error {
	defer func() { _ = snapshot.Close() }()

	kv, ok := s.KVStore.(stores.SnapshotKVStore)
	if !ok {
		return errKVStoreNoSnapshots
	}
	bts, err := io.ReadAll(snapshot)
	if err != nil {
		return err
	}
	items := map[string][]byte{}
	if r := json.Unmarshal(bts, &items); r != nil {
		return r
	}
	return kv.SetItems(items)
}

func (s *kvFSM) Reset() error {
	kv, ok := s.KVStore.(stores.SnapshotKVStore)
	if !ok {
		return errKVStoreNoSnapshots
	}
	return kv.Clear()
}

// kvSnapshot is the serialized store content captured by kvFSM.Snapshot.
type kvSnapshot []byte

// Persist writes the serialized content to the sink. Implements raft.FSMSnapshot.
func (s kvSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s); err != nil {
		return err
	}
	return sink.Close()
}

// Release implements raft.FSMSnapshot.
func (s kvSnapshot) Release() {}

func (s *kvFSM) store(uid uint64, cmd *pb.Store) (*pb.CommandResponse, error) {
	err := s.Store(cmd.Key, cmd.Data)
	return respStore(&pb.StoreResponse{Uid: uid}), err
}
