package worker

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func LoadCursor(path string) (int64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return 0, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

func SaveCursor(path string, id int64) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatInt(id, 10)+"\n"), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
