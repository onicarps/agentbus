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
	// Unique temp name avoids races when multiple workers write the same cursor.
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	payload := []byte(strconv.FormatInt(id, 10) + "\n")
	if _, err := f.Write(payload); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}
