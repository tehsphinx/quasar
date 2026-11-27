package exampleFSM

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/raft"
)

func cityCmd(t *testing.T, city City) []byte {
	t.Helper()
	data, err := json.Marshal(city)
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := json.Marshal(Change{Type: TypeCity, Data: data})
	if err != nil {
		t.Fatal(err)
	}
	return cmd
}

func snapshotOf(t *testing.T, musicians []Musician, cities []City) io.ReadCloser {
	t.Helper()
	lines := []string{sectMusicians}
	for _, m := range musicians {
		bts, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		lines = append(lines, string(bts))
	}
	lines = append(lines, sectCities)
	for _, c := range cities {
		bts, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		lines = append(lines, string(bts))
	}
	return io.NopCloser(strings.NewReader(strings.Join(lines, "\n") + "\n"))
}

func TestApplyCity(t *testing.T) {
	fsm := NewInMemoryFSM()
	if err := fsm.ApplyCmd(cityCmd(t, City{Name: "Bern", Country: "CH"})); err != nil {
		t.Fatal(err)
	}
	city, err := fsm.GetCityLocal("Bern")
	if err != nil {
		t.Fatal(err)
	}
	if city.Country != "CH" {
		t.Fatalf("country = %q, want CH", city.Country)
	}
}

// TestConcurrentCityReadWrite fails under -race when GetCityLocal reads without the lock.
func TestConcurrentCityReadWrite(t *testing.T) {
	fsm := NewInMemoryFSM()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 200 {
			if err := fsm.ApplyCmd(cityCmd(t, City{Name: "Bern", Country: "CH"})); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for range 200 {
			_, _ = fsm.GetCityLocal("Bern")
		}
	}()
	wg.Wait()
}

func TestRestoreReplacesState(t *testing.T) {
	fsm := NewInMemoryFSM()
	fsm.setMusician(Musician{Name: "old"})
	fsm.setCity(City{Name: "Old"})

	err := fsm.Restore(snapshotOf(t,
		[]Musician{{Name: "Miles", Age: 65}},
		[]City{{Name: "Bern", Country: "CH"}},
	))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := fsm.GetMusicianLocal("old"); err == nil {
		t.Error("musician from before the restore survived")
	}
	if _, err := fsm.GetCityLocal("Old"); err == nil {
		t.Error("city from before the restore survived")
	}
	if m, err := fsm.GetMusicianLocal("Miles"); err != nil || m.Age != 65 {
		t.Errorf("Miles = %+v, %v", m, err)
	}
	if c, err := fsm.GetCityLocal("Bern"); err != nil || c.Country != "CH" {
		t.Errorf("Bern = %+v, %v", c, err)
	}
}

func TestRestoreLargeEntry(t *testing.T) {
	fsm := NewInMemoryFSM()
	big := City{Name: "Big", Country: strings.Repeat("x", 128*1024)}

	err := fsm.Restore(snapshotOf(t, nil, []City{big, {Name: "After"}}))
	if err != nil {
		t.Fatal(err)
	}
	if c, err := fsm.GetCityLocal("Big"); err != nil || c.Country != big.Country {
		t.Errorf("Big not restored: %v", err)
	}
	if _, err := fsm.GetCityLocal("After"); err != nil {
		t.Errorf("entry after the large one not restored: %v", err)
	}
}

func TestRestoreErrorKeepsState(t *testing.T) {
	tests := map[string]string{
		"bad json":       sectCities + "\n{not json\n",
		"line too large": sectCities + "\n" + strings.Repeat("x", maxSnapshotLine+1) + "\n",
	}
	for name, snap := range tests {
		t.Run(name, func(t *testing.T) {
			fsm := NewInMemoryFSM()
			fsm.setCity(City{Name: "Bern"})

			if err := fsm.Restore(io.NopCloser(strings.NewReader(snap))); err == nil {
				t.Fatal("Restore returned nil on a broken snapshot")
			}
			if _, err := fsm.GetCityLocal("Bern"); err != nil {
				t.Errorf("state before the failed restore lost: %v", err)
			}
		})
	}
}

// failingSink is a raft.SnapshotSink whose writes fail.
type failingSink struct {
	err      error
	canceled bool
}

func (s *failingSink) Write([]byte) (int, error) { return 0, s.err }
func (s *failingSink) Close() error              { return nil }
func (s *failingSink) ID() string                { return "failing" }
func (s *failingSink) Cancel() error             { s.canceled = true; return nil }

func TestPersistWriteErrorCancelsSink(t *testing.T) {
	sink := &failingSink{err: errors.New("disk full")}
	snap := &snapshot{musicians: []Musician{{Name: "Miles"}}}

	if err := snap.Persist(sink); !errors.Is(err, sink.err) {
		t.Fatalf("Persist error = %v, want %v", err, sink.err)
	}
	if !sink.canceled {
		t.Error("Persist did not cancel the sink after a write error")
	}
}

func TestGetMusicianLocalReturnsCopy(t *testing.T) {
	fsm := NewInMemoryFSM()
	fsm.setMusician(Musician{Name: "Miles", Instruments: []string{"trumpet"}})

	m, err := fsm.GetMusicianLocal("Miles")
	if err != nil {
		t.Fatal(err)
	}
	m.Instruments[0] = "kazoo"

	m, err = fsm.GetMusicianLocal("Miles")
	if err != nil {
		t.Fatal(err)
	}
	if m.Instruments[0] != "trumpet" {
		t.Fatalf("instrument = %q, want trumpet", m.Instruments[0])
	}
}

// TestConcurrentMusicianMutateSnapshot fails under -race when GetMusicianLocal hands out
// the FSM's Instruments array.
func TestConcurrentMusicianMutateSnapshot(t *testing.T) {
	fsm := NewInMemoryFSM()
	fsm.setMusician(Musician{Name: "Miles", Instruments: []string{"trumpet"}})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 200 {
			m, err := fsm.GetMusicianLocal("Miles")
			if err != nil {
				t.Error(err)
				return
			}
			m.Instruments[0] = "kazoo"
		}
	}()
	go func() {
		defer wg.Done()
		for range 200 {
			snap, err := fsm.Snapshot()
			if err != nil {
				t.Error(err)
				return
			}
			if err := snap.Persist(&raft.DiscardSnapshotSink{}); err != nil {
				t.Error(err)
				return
			}
			snap.Release()
		}
	}()
	wg.Wait()
}
