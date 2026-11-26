package agentgo

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// ProjectSnapshot bundles an agent's serializable RunState FSM and its MemoryBank
// into a single lightweight JSON manifest (Spec §8.2).
type ProjectSnapshot struct {
	RunState RunState       `json:"run_state"`
	Memory   MemorySnapshot `json:"memory"`
}

// SaveSnapshot atomically writes a ProjectSnapshot (RunState + optional MemoryBank) to path.
func SaveSnapshot(path string, state RunState, bank *MemoryBank) error {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	snap := ProjectSnapshot{
		RunState: state.Clone(),
	}
	if bank != nil {
		snap.Memory = bank.Snapshot()
	}
	raw, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// LoadSnapshot reads a ProjectSnapshot from path. If bank is non-nil, its contents are restored.
func LoadSnapshot(path string, bank *MemoryBank) (ProjectSnapshot, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ProjectSnapshot{}, err
	}
	var snap ProjectSnapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return ProjectSnapshot{}, err
	}
	if bank != nil {
		bank.Restore(snap.Memory)
	}
	return snap, nil
}

