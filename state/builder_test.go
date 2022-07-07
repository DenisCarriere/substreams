package state

import (
	"testing"

	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"

	"github.com/stretchr/testify/assert"
)

func TestApplyDelta(t *testing.T) {

	tests := []struct {
		name       string
		deltas     []*pbsubstreams.StoreDelta
		expectedKV map[string][]byte
	}{
		{
			name: "creates",
			deltas: []*pbsubstreams.StoreDelta{
				{
					Operation: pbsubstreams.StoreDelta_CREATE,
					Key:       "k1",
					NewValue:  []byte("v1"),
				},
				{
					Operation: pbsubstreams.StoreDelta_CREATE,
					Key:       "k2",
					NewValue:  []byte("v2"),
				},
			},
			expectedKV: map[string][]byte{
				"k1": []byte("v1"),
				"k2": []byte("v2"),
			},
		},
		{
			name: "update",
			deltas: []*pbsubstreams.StoreDelta{
				{
					Operation: pbsubstreams.StoreDelta_CREATE,
					Key:       "k1",
					NewValue:  []byte("v1"),
				},
				{
					Operation: pbsubstreams.StoreDelta_UPDATE,
					Key:       "k1",
					OldValue:  []byte("v1"),
					NewValue:  []byte("v2"),
				},
			},
			expectedKV: map[string][]byte{
				"k1": []byte("v2"),
			},
		},
		{
			name: "delete",
			deltas: []*pbsubstreams.StoreDelta{
				{
					Operation: pbsubstreams.StoreDelta_CREATE,
					Key:       "k1",
					NewValue:  []byte("v1"),
				},
				{
					Operation: pbsubstreams.StoreDelta_CREATE,
					Key:       "k2",
					NewValue:  []byte("v2"),
				},
				{
					Operation: pbsubstreams.StoreDelta_DELETE,
					Key:       "k1",
					OldValue:  []byte("v1"),
				},
			},
			expectedKV: map[string][]byte{
				"k2": []byte("v2"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := &Store{
				KV: make(map[string][]byte),
			}
			for _, delta := range test.deltas {
				s.ApplyDelta(delta)
			}
			assert.Equal(t, test.expectedKV, s.KV)
		})
	}
}

func TestApplyDeltaReverse(t *testing.T) {

	tests := []struct {
		name       string
		initialKV  map[string][]byte
		deltas     []*pbsubstreams.StoreDelta
		expectedKV map[string][]byte
	}{
		{
			name: "creates",
			initialKV: map[string][]byte{
				"k1": []byte("v1"),
				"k2": []byte("v2"),
				"k3": []byte("v3"),
			},
			deltas: []*pbsubstreams.StoreDelta{
				{
					Operation: pbsubstreams.StoreDelta_CREATE,
					Key:       "k1",
					NewValue:  []byte("v1"),
				},
				{
					Operation: pbsubstreams.StoreDelta_CREATE,
					Key:       "k2",
					NewValue:  []byte("v2"),
				},
			},
			expectedKV: map[string][]byte{
				"k3": []byte("v3"),
			},
		},
		{
			name: "updates",
			initialKV: map[string][]byte{
				"k1": []byte("v1new"),
				"k2": []byte("v2new"),
				"k3": []byte("v3"),
			},
			deltas: []*pbsubstreams.StoreDelta{
				{
					Operation: pbsubstreams.StoreDelta_UPDATE,
					Key:       "k1",
					NewValue:  []byte("v1new"),
					OldValue:  []byte("v1old"),
				},
				{
					Operation: pbsubstreams.StoreDelta_UPDATE,
					Key:       "k2",
					NewValue:  []byte("v2new"),
					OldValue:  []byte("v2old"),
				},
			},
			expectedKV: map[string][]byte{
				"k1": []byte("v1old"),
				"k2": []byte("v2old"),
				"k3": []byte("v3"),
			},
		},
		{
			name: "deletes",
			initialKV: map[string][]byte{
				"k3": []byte("v3"),
			},
			deltas: []*pbsubstreams.StoreDelta{
				{
					Operation: pbsubstreams.StoreDelta_DELETE,
					Key:       "k1",
					OldValue:  []byte("v1"),
				},
				{
					Operation: pbsubstreams.StoreDelta_DELETE,
					Key:       "k2",
					OldValue:  []byte("v2"),
				},
			},
			expectedKV: map[string][]byte{
				"k1": []byte("v1"),
				"k2": []byte("v2"),
				"k3": []byte("v3"),
			},
		},
		{
			name: "updates_ordered",
			initialKV: map[string][]byte{
				"k1": []byte("v1bisbis"),
			},
			deltas: []*pbsubstreams.StoreDelta{
				{
					Operation: pbsubstreams.StoreDelta_UPDATE,
					Key:       "k1",
					NewValue:  []byte("v1bis"),
					OldValue:  []byte("v1"),
				},
				{
					Operation: pbsubstreams.StoreDelta_UPDATE,
					Key:       "k1",
					NewValue:  []byte("v1bisbis"),
					OldValue:  []byte("v1bis"),
				},
			},
			expectedKV: map[string][]byte{
				"k1": []byte("v1"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := &Store{
				KV: test.initialKV,
			}
			s.ApplyDeltaReverse(test.deltas)
			assert.Equal(t, test.expectedKV, s.KV)
		})
	}
}
func TestStateBuilder(t *testing.T) {
	s := mustNewBuilder(t, "b", 0, "modulehash.1", pbsubstreams.Module_KindStore_UPDATE_POLICY_UNSET, "", nil)

	s.Set(0, "1", "val1")
	s.Set(1, "1", "val2")
	s.Set(3, "1", "val3")
	s.Flush()
	s.Set(0, "1", "val4")
	s.Set(1, "1", "val5")
	s.Set(3, "1", "val6")
	s.Del(4, "1")
	s.Set(5, "1", "val7")

	val, found := s.GetFirst("1")
	assert.Equal(t, "val3", string(val))
	assert.True(t, found)

	val, found = s.GetAt(0, "1")
	assert.Equal(t, "val4", string(val))
	assert.True(t, found)

	val, found = s.GetAt(1, "1")
	assert.Equal(t, "val5", string(val))
	assert.True(t, found)

	val, found = s.GetAt(3, "1")
	assert.Equal(t, "val6", string(val))
	assert.True(t, found)

	val, found = s.GetAt(4, "1")
	assert.Nil(t, val)
	assert.False(t, found)

	val, found = s.GetAt(5, "1")
	assert.Equal(t, "val7", string(val))
	assert.True(t, found)

	val, found = s.GetLast("1")
	assert.Equal(t, "val7", string(val))
	assert.True(t, found)
}
