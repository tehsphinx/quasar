package exampleFSM

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar"
	"golang.org/x/exp/maps"
)

type Musician struct {
	Name        string   `json:"name"`
	Age         int      `json:"age"`
	Instruments []string `json:"instruments"`
}

type City struct {
	Name    string `json:"name"`
	Country string `json:"country"`
}

type ChangeType int

const (
	TypeUnknown ChangeType = iota
	TypeMusician
	TypeCity
)

type Change struct {
	Type ChangeType `json:"type"`
	Data []byte     `json:"data"`
}

func NewInMemoryFSM() *InMemoryFSM {
	return &InMemoryFSM{
		musicians: map[string]Musician{},
		cities:    map[string]City{},
	}
}

var _ quasar.FSM = (*InMemoryFSM)(nil)

type InMemoryFSM struct {
	fsm *quasar.FSMInjector

	musiciansM sync.RWMutex
	musicians  map[string]Musician
	citiesM    sync.RWMutex
	cities     map[string]City
}

func (s *InMemoryFSM) SetMusician(ctx context.Context, musician Musician) error {
	bts, err := json.Marshal(musician)
	if err != nil {
		return err
	}

	bts, err = json.Marshal(Change{
		Type: TypeMusician,
		Data: bts,
	})
	if err != nil {
		return err
	}

	_, err = s.fsm.Store(ctx, bts)
	return err
}

// GetMusicianMaster uses the provided WaitForMasterLatest function. It checks with the master
// first to get the latest uid the master has accepted and then waits for that uid to be applied
// locally. Only then it uses GetMusicianLocal to read the value from local cache. This way
// it makes sure that the node is properly connected to the RAFT network and not behind.
func (s *InMemoryFSM) GetMusicianMaster(ctx context.Context, name string) (Musician, error) {
	if err := s.fsm.WaitForMasterLatest(ctx); err != nil {
		return Musician{}, err
	}
	return s.GetMusicianLocal(name)
}

// GetMusicianKnownLatest uses the provided WaitForKnownLatest function. It checks the latest
// announced uid then waits for that uid to be applied locally. Only then it uses GetMusicianLocal
// to read the value from local cache. This way it makes sure that all known uids are applied first
// before reading. It does not however make sure the node is properly connected. It might not receive
// updates from the master anymore.
func (s *InMemoryFSM) GetMusicianKnownLatest(ctx context.Context, name string) (Musician, error) {
	if err := s.fsm.WaitForKnownLatest(ctx); err != nil {
		return Musician{}, err
	}
	return s.GetMusicianLocal(name)
}

// GetMusicianLocal gets a value from the local cache. There is no consistency guarantee here.
func (s *InMemoryFSM) GetMusicianLocal(name string) (Musician, error) {
	s.musiciansM.RLock()
	defer s.musiciansM.RUnlock()

	musician, ok := s.musicians[name]
	if !ok {
		return Musician{}, fmt.Errorf("musician %s not found", name)
	}
	return musician, nil
}

// GetCityLocal gets a value from the local cache. There is no consistency guarantee here.
func (s *InMemoryFSM) GetCityLocal(name string) (City, error) {
	city, ok := s.cities[name]
	if !ok {
		return City{}, fmt.Errorf("city %s not found", name)
	}
	return city, nil
}

// ApplyCmd unmarshalls incoming changes and stores the data in its native types.
func (s *InMemoryFSM) ApplyCmd(cmd []byte) error {
	var change Change
	if err := json.Unmarshal(cmd, &change); err != nil {
		return err
	}

	switch change.Type {
	case TypeMusician:
		var musician Musician
		if err := json.Unmarshal(change.Data, &musician); err != nil {
			return err
		}
		s.setMusician(musician)
		return nil
	case TypeCity:
		var city City
		if err := json.Unmarshal(change.Data, &city); err != nil {
			return err
		}
		s.setCity(city)
		return nil
	default:
		return errors.New("unknown apply type")
	}
}

func (s *InMemoryFSM) setMusician(musician Musician) {
	s.musiciansM.Lock()
	defer s.musiciansM.Unlock()

	s.musicians[musician.Name] = musician
}

func (s *InMemoryFSM) setCity(city City) {
	s.citiesM.Lock()
	defer s.citiesM.RUnlock()

	s.cities[city.Name] = city
}

// Inject must be implemented to receive the FSMInjector object. It provides the necessary
// functions to call into the cache without cluttering the Cache API. It is called in the
// quasar.NewCache function.
func (s *InMemoryFSM) Inject(fsm *quasar.FSMInjector) {
	s.fsm = fsm
}

type snapshot struct {
	musicians []Musician
	cities    []City
}

func (s *InMemoryFSM) Snapshot() (raft.FSMSnapshot, error) {
	s.musiciansM.RLock()
	s.citiesM.RLock()
	defer func() {
		s.citiesM.RUnlock()
		s.musiciansM.RUnlock()
	}()

	return &snapshot{
		musicians: maps.Values(s.musicians),
		cities:    maps.Values(s.cities),
	}, nil
}

const (
	sectMusicians = "--> musicians <--"
	sectCities    = "--> cities <--"
)

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if err := s.newSection(sink, sectMusicians); err != nil {
		return err
	}
	for _, musician := range s.musicians {
		if err := s.writeValue(sink, musician); err != nil {
			return err
		}
	}

	if err := s.newSection(sink, sectCities); err != nil {
		return err
	}
	for _, city := range s.cities {
		if err := s.writeValue(sink, city); err != nil {
			return err
		}
	}
	return nil
}

func (s *InMemoryFSM) Restore(reader io.ReadCloser) error {
	scanner := bufio.NewScanner(reader)
	var dataType string
	for scanner.Scan() {
		bts := scanner.Bytes()
		switch string(bts) {
		case sectMusicians:
			dataType = sectMusicians
			continue
		case sectCities:
			dataType = sectCities
			continue
		}

		switch dataType {
		case sectMusicians:
			var musician Musician
			if err := json.Unmarshal(bts, &musician); err != nil {
				return err
			}
			s.setMusician(musician)
		case sectCities:
			var city City
			if err := json.Unmarshal(bts, &city); err != nil {
				return err
			}
			s.setCity(city)
		}
	}
	return nil
}

func (s *InMemoryFSM) Reset() error {
	s.musiciansM.Lock()
	s.citiesM.Lock()
	defer func() {
		s.citiesM.Unlock()
		s.musiciansM.Unlock()
	}()

	s.cities = map[string]City{}
	s.musicians = map[string]Musician{}
	return nil
}

func (s *snapshot) newSection(sink raft.SnapshotSink, section string) error {
	if _, err := sink.Write([]byte(section)); err != nil {
		return err
	}
	_, err := sink.Write([]byte{'\n'})
	return err
}

func (s *snapshot) writeValue(sink raft.SnapshotSink, value any) error {
	const newline = "\n"

	bts, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if _, r := sink.Write(bts); r != nil {
		return r
	}
	if _, r := sink.Write([]byte(newline)); r != nil {
		return r
	}
	return nil
}

func (s *snapshot) Release() {
	s.musicians = nil
	s.cities = nil
}
