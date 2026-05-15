package backup

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// ChainState tracks the latest successful backup in a named chain (local staging).
type ChainState struct {
	LastBackupID string   `json:"last_backup_id"`
	LastType     Type     `json:"last_type"`
	LastOplogTS  *OplogTS `json:"last_oplog_ts,omitempty"`
}

func chainStatePath(staging, chain string) string {
	return filepath.Join(staging, chain, "_chain_state.json")
}

func loadChainState(staging, chain string) (*ChainState, error) {
	p := chainStatePath(staging, chain)
	data, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var st ChainState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

func saveChainState(staging, chain string, m *Manifest) error {
	if m == nil {
		return fmt.Errorf("manifest nil")
	}
	st := &ChainState{
		LastBackupID: m.BackupID,
		LastType:     m.Type,
		LastOplogTS:  m.LastOplogTS,
	}
	dir := filepath.Join(staging, chain)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	tmp := chainStatePath(staging, chain) + ".tmp"
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0o640); err != nil {
		return err
	}
	return os.Rename(tmp, chainStatePath(staging, chain))
}
