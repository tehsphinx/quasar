package quasar

import (
	"context"
	"testing"
	"time"

	"github.com/matryer/is"
)

func TestSingleCache(t *testing.T) {
	type test struct {
		name      string
		storeVals map[string]string
	}
	tests := []test{
		{
			name: "sub test",
			storeVals: map[string]string{
				"key1": "val1",
				"key2": "val2",
				"key3": "val3",
			},
		},
	}

	ctxMain, cancelMain := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelMain()

	asrtMain := is.New(t)

	cache, err := NewCache(WithTCPRaft(28224))
	asrtMain.NoErr(err)

	err = cache.WaitReady(ctxMain)
	asrtMain.NoErr(err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			asrt := asrtMain.New(t)

			for k, v := range tt.storeVals {
				_, r := cache.Store(k, []byte(v))
				asrt.NoErr(r)
			}

			for k, v := range tt.storeVals {
				got, r := cache.Load(k)
				asrt.NoErr(r)

				asrt.Equal(got, []byte(v))
			}
		})
	}
}
