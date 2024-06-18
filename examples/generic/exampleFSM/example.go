package exampleFSM

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/tehsphinx/quasar"
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

type InMemoryFSM struct {
	fsm *quasar.FSMInjector

	musicians map[string]Musician
	cities    map[string]City
}

func (s *InMemoryFSM) SetMusician(musician Musician) error {
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

	_, err = s.fsm.Store(bts)
	return err
}

// GetMusicianMaster uses the provided WaitForMasterLatest function. It checks with the master
// first to get the latest uid the master has accepted and then waits for that uid to be applied
// locally. Only then it uses GetMusicianLocal to read the value from local cache. This way
// it makes sure that the node is properly connected to the RAFT network and not behind.
func (s *InMemoryFSM) GetMusicianMaster(name string) (Musician, error) {
	if err := s.fsm.WaitForMasterLatest(); err != nil {
		return Musician{}, err
	}
	return s.GetMusicianLocal(name)
}

// GetMusicianKnownLatest uses the provided WaitForKnownLatest function. It checks the latest
// announced uid then waits for that uid to be applied locally. Only then it uses GetMusicianLocal
// to read the value from local cache. This way it makes sure that all known uids are applied first
// before reading. It does not however make sure the node is properly connected. It might not receive
// updates from the master anymore.
func (s *InMemoryFSM) GetMusicianKnownLatest(name string) (Musician, error) {
	s.fsm.WaitForKnownLatest()
	return s.GetMusicianLocal(name)
}

// GetMusicianLocal gets a value from the local cache. There is no consistency guarantee here.
func (s *InMemoryFSM) GetMusicianLocal(name string) (Musician, error) {
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

// Inject must be implemented to receive the FSMInjector object. It provides the necessary
// functions to call into the cache without cluttering the Cache API. It is called in the
// quasar.NewCache function.
func (s *InMemoryFSM) Inject(fsm *quasar.FSMInjector) {
	s.fsm = fsm
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
		s.musicians[musician.Name] = musician
		return nil
	case TypeCity:
		var city City
		if err := json.Unmarshal(change.Data, &city); err != nil {
			return err
		}
		s.cities[city.Name] = city
		return nil
	default:
		return errors.New("unknown apply type")
	}
}
