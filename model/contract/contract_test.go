package contract

import (
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
)

// The verdict on two colliding edits: a merge that reports a conflict, with
// a reason for the person resolving it, passes; one that reports none took
// a side in silence and fails; one whose conflict has no reason fails.
func TestCollisionVerdict(t *testing.T) {
	if err := collisionVerdict(model.MergeResult{Conflicts: []model.Conflict{{Location: []byte("x"), Reason: "both sides changed it"}}}); err != nil {
		t.Errorf("a merge reporting a reasoned conflict was refused: %v", err)
	}
	if err := collisionVerdict(model.MergeResult{}); err == nil {
		t.Error("a merge of colliding edits that reported no conflict passed: the model took a side in silence")
	}
	if err := collisionVerdict(model.MergeResult{Conflicts: []model.Conflict{{Location: []byte("x")}}}); err == nil {
		t.Error("a conflict with no reason passed")
	}
}
