package identity

import (
	"bytes"
	"testing"
)

// FuzzCanonicalizeJSON exercises the AgentID wire boundary: duplicate keys,
// unsafe numbers, malformed Unicode, and parser ambiguity must return errors,
// never panic or produce unstable canonical bytes.
func FuzzCanonicalizeJSON(f *testing.F) {
	for _, seed := range [][]byte{
		[]byte(`{"from":"codex","n":1}`),
		[]byte(`{"from":"codex","from":"agy"}`),
		[]byte(`{"n":9007199254740993}`),
		[]byte(`{"s":"\ud800"}`),
		[]byte(`[]`),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<20 {
			t.Skip()
		}
		first, err := CanonicalizeJSON(raw)
		if err != nil {
			return
		}
		second, err := CanonicalizeJSON(raw)
		if err != nil {
			t.Fatalf("accepted input became invalid: %v", err)
		}
		if !bytes.Equal(first, second) {
			t.Fatal("canonicalization is not deterministic")
		}
	})
}

// FuzzVerifyEd25519 exercises malformed canonical JSON, signatures, and public
// keys together. All attacker-controlled byte strings must fail as data, not
// crash the broker/client process.
func FuzzVerifyEd25519(f *testing.F) {
	f.Add([]byte(`{"from":"codex"}`), "", "")
	f.Add([]byte(`{"n":9007199254740993}`), "not-base64", "not-base64")
	f.Fuzz(func(t *testing.T, unsigned []byte, signature, publicKey string) {
		if len(unsigned)+len(signature)+len(publicKey) > 1<<20 {
			t.Skip()
		}
		_, _ = VerifyEd25519(unsigned, signature, publicKey)
	})
}
