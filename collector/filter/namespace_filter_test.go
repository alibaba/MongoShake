package filter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNamespaceFilterRuleMatching(t *testing.T) {
	tests := []struct {
		name      string
		white     []string
		black     []string
		namespace string
		filtered  bool
	}{
		{name: "no rules", namespace: "source.items"},
		{name: "white exact match", white: []string{"source.items"}, namespace: "source.items"},
		{name: "white database prefix", white: []string{"source"}, namespace: "source.items"},
		{name: "white collection prefix", white: []string{"source.items"}, namespace: "source.items.archive"},
		{name: "white prefix boundary", white: []string{"source.items"}, namespace: "source.items2", filtered: true},
		{name: "white literal dot", white: []string{"source.items"}, namespace: "sourceXitems", filtered: true},
		{name: "white regex match", white: []string{"source.events[0-9]"}, namespace: "source.events7"},
		{name: "white regex miss", white: []string{"source.events[0-9]"}, namespace: "source.eventsX", filtered: true},
		{name: "black exact match", black: []string{"source.items"}, namespace: "source.items", filtered: true},
		{name: "black database prefix", black: []string{"source"}, namespace: "source.items", filtered: true},
		{name: "black regex prefix", black: []string{"source.events[0-9]"}, namespace: "source.events7.archive", filtered: true},
		{name: "black regex miss", black: []string{"source.events[0-9]"}, namespace: "source.eventsX"},
		{name: "black overrides white", white: []string{"source"}, black: []string{"source.items"}, namespace: "source.items", filtered: true},
		{name: "invalid white rejects", white: []string{"source.["}, namespace: "source.items", filtered: true},
		{name: "invalid black allows", black: []string{"source.["}, namespace: "source.items"},
		{name: "invalid black with valid white", white: []string{"source"}, black: []string{"source.["}, namespace: "source.items"},
		{name: "invalid white command exception", white: []string{"source.["}, namespace: "source.$cmd"},
		{name: "white command exception", white: []string{"source.items"}, namespace: "source.$cmd"},
		{name: "system views exception", white: []string{"source.items"}, namespace: "other.system.views"},
		{name: "bucket logical name match", white: []string{"source.events[0-9]"}, namespace: "source.system.buckets.events7"},
		{name: "bucket logical name miss", white: []string{"source.events[0-9]"}, namespace: "source.system.buckets.eventsX", filtered: true},
		{name: "bucket blacklist match", black: []string{"source.events[0-9]"}, namespace: "source.system.buckets.events7", filtered: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewNamespaceFilter(tt.white, tt.black)
			require.Equal(t, tt.filtered, f.FilterNs(tt.namespace))
		})
	}
}

func BenchmarkNamespaceFilterNs(b *testing.B) {
	for _, size := range []int{1, 4, 23} {
		rules := make([]string, size)
		for i := range rules {
			rules[i] = fmt.Sprintf("source.collection%02d", i)
		}
		tests := []struct {
			name      string
			white     []string
			black     []string
			namespace string
			filtered  bool
		}{
			{name: "white_match", white: rules, namespace: rules[size-1]},
			{name: "white_miss", white: rules, namespace: "other.collection", filtered: true},
			{name: "black_match", black: rules, namespace: rules[size-1], filtered: true},
			{name: "black_miss", black: rules, namespace: "other.collection"},
		}
		for _, tt := range tests {
			b.Run(fmt.Sprintf("%s/%d", tt.name, size), func(b *testing.B) {
				f := NewNamespaceFilter(tt.white, tt.black)
				if f.FilterNs(tt.namespace) != tt.filtered {
					b.Fatal("unexpected filtering result")
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					f.FilterNs(tt.namespace)
				}
			})
		}
	}
}
