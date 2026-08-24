// Package identity implements the AgentID RFC 8785 and Ed25519 byte boundary.
package identity

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/cyberphone/json-canonicalization/go/src/webpki.org/jsoncanonicalizer"
	"golang.org/x/text/unicode/norm"
)

var maxSafeInteger = big.NewInt(9007199254740991)

// CanonicalizeJSON validates the shared AgentID JSON domain and returns RFC
// 8785 bytes. Duplicate keys are rejected before object construction.
func CanonicalizeJSON(raw []byte) ([]byte, error) {
	if err := rejectUnpairedEscapedSurrogates(raw); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := parseValue(decoder, nil, "$", map[string]struct{}{}); err != nil {
		return nil, err
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("trailing_json_token: %v", token)
		}
		return nil, fmt.Errorf("invalid_json: %w", err)
	}
	canonical, err := jsoncanonicalizer.Transform(raw)
	if err != nil {
		return nil, fmt.Errorf("jcs_error: %w", err)
	}
	return canonical, nil
}

func rejectUnpairedEscapedSurrogates(raw []byte) error {
	inString := false
	for i := 0; i < len(raw); {
		if raw[i] == '"' {
			inString = !inString
			i++
			continue
		}
		if !inString || raw[i] != '\\' {
			i++
			continue
		}
		if i+1 >= len(raw) {
			return fmt.Errorf("invalid_json_escape")
		}
		if raw[i+1] != 'u' {
			i += 2
			continue
		}
		if i+6 > len(raw) {
			return fmt.Errorf("invalid_unicode_escape")
		}
		unit, err := strconv.ParseUint(string(raw[i+2:i+6]), 16, 16)
		if err != nil {
			return fmt.Errorf("invalid_unicode_escape")
		}
		if unit >= 0xDC00 && unit <= 0xDFFF {
			return fmt.Errorf("invalid_unicode_scalar")
		}
		if unit >= 0xD800 && unit <= 0xDBFF {
			if i+12 > len(raw) || raw[i+6] != '\\' || raw[i+7] != 'u' {
				return fmt.Errorf("invalid_unicode_scalar")
			}
			low, lowErr := strconv.ParseUint(string(raw[i+8:i+12]), 16, 16)
			if lowErr != nil || low < 0xDC00 || low > 0xDFFF {
				return fmt.Errorf("invalid_unicode_scalar")
			}
			i += 12
			continue
		}
		i += 6
	}
	return nil
}

func parseValue(
	decoder *json.Decoder,
	first json.Token,
	path string,
	_ map[string]struct{},
) error {
	token := first
	var err error
	if token == nil {
		token, err = decoder.Token()
		if err != nil {
			return fmt.Errorf("invalid_json: %w", err)
		}
	}
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			seen := map[string]struct{}{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return fmt.Errorf("invalid_json: %w", err)
				}
				key, ok := keyToken.(string)
				if !ok {
					return fmt.Errorf("non_string_key: %s", path)
				}
				if err := validateString(key, path+".<key>"); err != nil {
					return err
				}
				if _, exists := seen[key]; exists {
					return fmt.Errorf("duplicate_json_key: %s", key)
				}
				seen[key] = struct{}{}
				if err := parseValue(decoder, nil, path+"."+key, nil); err != nil {
					return err
				}
			}
			closing, err := decoder.Token()
			if err != nil || closing != json.Delim('}') {
				return fmt.Errorf("invalid_json_object: %s", path)
			}
		case '[':
			index := 0
			for decoder.More() {
				if err := parseValue(decoder, nil, fmt.Sprintf("%s[%d]", path, index), nil); err != nil {
					return err
				}
				index++
			}
			closing, err := decoder.Token()
			if err != nil || closing != json.Delim(']') {
				return fmt.Errorf("invalid_json_array: %s", path)
			}
		default:
			return fmt.Errorf("unexpected_delimiter: %s", path)
		}
	case string:
		return validateString(value, path)
	case json.Number:
		text := value.String()
		if !strings.ContainsAny(text, ".eE") {
			integer, ok := new(big.Int).SetString(text, 10)
			if !ok {
				return fmt.Errorf("invalid_number: %s", path)
			}
			if new(big.Int).Abs(integer).Cmp(maxSafeInteger) > 0 {
				return fmt.Errorf("unsafe_integer: %s", path)
			}
		}
	case bool, nil:
		return nil
	default:
		return fmt.Errorf("unsupported_json_type: %s", path)
	}
	return nil
}

func validateString(value, path string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("invalid_unicode_scalar: %s", path)
	}
	if !norm.NFC.IsNormalString(value) {
		return fmt.Errorf("non_nfc_string: %s", path)
	}
	return nil
}

// VerifyEd25519 verifies an unpadded base64url AgentID signature and public key.
func VerifyEd25519(unsigned []byte, signature, publicKey string) (bool, error) {
	canonical, err := CanonicalizeJSON(unsigned)
	if err != nil {
		return false, err
	}
	public, err := base64.RawURLEncoding.DecodeString(publicKey)
	if err != nil || len(public) != ed25519.PublicKeySize {
		return false, fmt.Errorf("invalid_ed25519_public_key")
	}
	sig, err := base64.RawURLEncoding.DecodeString(signature)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return false, fmt.Errorf("invalid_ed25519_signature")
	}
	return ed25519.Verify(ed25519.PublicKey(public), canonical, sig), nil
}
