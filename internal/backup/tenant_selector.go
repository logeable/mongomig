package backup

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
)

var restoreTenantKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_]{42}$`)

// ResolveRestoreTenantKeys combines repeated --tenant-key values with the
// optional --tenant-file contents. The result is de-duplicated while
// preserving first-seen order.
func ResolveRestoreTenantKeys(keys []string, file string) ([]string, error) {
	var values []string
	values = append(values, keys...)
	if strings.TrimSpace(file) != "" {
		f, err := os.Open(file)
		if err != nil {
			return nil, fmt.Errorf("open tenant file %q: %w", file, err)
		}
		defer func() { _ = f.Close() }()

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			values = append(values, line)
		}
		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("read tenant file %q: %w", file, err)
		}
	}

	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if !restoreTenantKeyPattern.MatchString(value) {
			return nil, fmt.Errorf("invalid tenant_key %q: expected exactly 42 ASCII letters/digits/underscore characters", value)
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out, nil
}
