package exampleFSM

import (
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
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
