package backup

import (
	"fmt"
	"strings"
)

// NSSpec is a database.collection pair.
type NSSpec struct {
	DB   string
	Coll string
}

func (n NSSpec) String() string {
	return n.DB + "." + n.Coll
}

// ParseCollectionSpecs parses comma-separated "db.collection" entries.
func ParseCollectionSpecs(csv string) ([]NSSpec, error) {
	csv = strings.TrimSpace(csv)
	if csv == "" {
		return nil, fmt.Errorf("collections is empty")
	}
	var out []NSSpec
	for _, part := range strings.Split(csv, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		db, coll, ok := strings.Cut(part, ".")
		db, coll = strings.TrimSpace(db), strings.TrimSpace(coll)
		if !ok || db == "" || coll == "" {
			return nil, fmt.Errorf("invalid collection %q (want db.collection)", part)
		}
		out = append(out, NSSpec{DB: db, Coll: coll})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no collections parsed")
	}
	return out, nil
}
