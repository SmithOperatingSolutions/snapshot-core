package contract

import "testing"

// The suite's listing verdicts: byte order is byte order; a backend in a
// total order of its own (SeaweedFS lists a directory's children right
// after the directory) passes AnyTotalOrder with every name once and its
// prefixes in the order of its full listing, and fails it on a name
// missing, doubled or out of that order.
func TestListingVerdicts(t *testing.T) {
	stored := []string{"a-b", "a/b", "a/b/c", "a/c", "a0", "b"}
	byteOrder := []string{"a-b", "a/b", "a/b/c", "a/c", "a0", "b"}
	treeOrder := []string{"a/b/c", "a/b", "a/c", "a-b", "a0", "b"}
	if err := orderVerdict(ByteOrder, byteOrder, stored); err != nil {
		t.Errorf("byte order under ByteOrder: %v", err)
	}
	if err := orderVerdict(ByteOrder, treeOrder, stored); err == nil {
		t.Error("tree order passed ByteOrder")
	}
	if err := orderVerdict(AnyTotalOrder, treeOrder, stored); err != nil {
		t.Errorf("tree order under AnyTotalOrder: %v", err)
	}
	for name, got := range map[string][]string{
		"a name missing": {"a/b/c", "a/b", "a/c", "a-b", "a0"},
		"a name doubled": {"a/b/c", "a/b", "a/c", "a-b", "a0", "b", "b"},
		"a stranger":     {"a/b/c", "a/b", "a/c", "a-b", "a0", "b", "zzz"},
	} {
		if err := orderVerdict(AnyTotalOrder, got, stored); err == nil {
			t.Errorf("%s passed AnyTotalOrder", name)
		}
	}
	under := []string{"a/b/c", "a/b", "a/c"}
	if err := prefixVerdict(AnyTotalOrder, treeOrder, "a/", under, nil); err != nil {
		t.Errorf("a prefix listing in the full listing's order: %v", err)
	}
	if err := prefixVerdict(AnyTotalOrder, treeOrder, "a/", []string{"a/b", "a/b/c", "a/c"}, nil); err == nil {
		t.Error("a prefix listing in another order than the full listing passed AnyTotalOrder")
	}
	if err := prefixVerdict(ByteOrder, treeOrder, "a/", []string{"a/b", "a/b/c", "a/c"}, []string{"a/b", "a/b/c", "a/c"}); err != nil {
		t.Errorf("a prefix listing in byte order under ByteOrder: %v", err)
	}
}
