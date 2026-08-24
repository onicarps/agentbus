package identity

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type fixture struct {
	PublicKey       string `json:"public_key"`
	CanonicalSHA256 string `json:"canonical_sha256"`
	Envelope        struct {
		Signed    json.RawMessage `json:"signed"`
		Signature string          `json:"signature"`
	} `json:"envelope"`
}

func TestCrossLanguageFixture(t *testing.T) {
	path := filepath.Join("..", "..", "..", "tests", "fixtures", "agentid", "cross_language_v1.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var data fixture
	if err := json.Unmarshal(raw, &data); err != nil {
		t.Fatal(err)
	}
	canonical, err := CanonicalizeJSON(data.Envelope.Signed)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(canonical)
	if got := hex.EncodeToString(digest[:]); got != data.CanonicalSHA256 {
		t.Fatalf("canonical digest=%s want=%s", got, data.CanonicalSHA256)
	}
	ok, err := VerifyEd25519(data.Envelope.Signed, data.Envelope.Signature, data.PublicKey)
	if err != nil || !ok {
		t.Fatalf("signature verification ok=%v err=%v", ok, err)
	}
}

func TestRejectsAmbiguousInputs(t *testing.T) {
	tests := []string{
		`{"from":"codex","from":"agy"}`,
		`{"n":9007199254740993}`,
		"{\"s\":\"e\u0301\"}",
	}
	for _, raw := range tests {
		if _, err := CanonicalizeJSON([]byte(raw)); err == nil {
			t.Fatalf("expected rejection for %s", raw)
		}
	}
}
