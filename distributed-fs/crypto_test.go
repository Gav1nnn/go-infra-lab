package main

import (
	"bytes"
	"testing"
)

// TestCopyEncryptDecrypt verifies that encryption followed by decryption
// returns the original plaintext.
func TestCopyEncryptDecrypt(t *testing.T) {
	payload := "Foo not bar"
	src := bytes.NewReader([]byte(payload))
	dst := new(bytes.Buffer)
	key := newEncryptionKey()

	_, err := copyEncrypt(key, src, dst)
	if err != nil {
		t.Error(err)
	}

	out := new(bytes.Buffer)
	nw, err := copyDecrypt(key, dst, out)
	if err != nil {
		t.Error(err)
	}

	if nw != 16+len(payload) {
		t.Errorf("expected %d bytes written, got %d", 16+len(payload), nw)
	}

	if out.String() != payload {
		t.Errorf("decryption produced wrong content: got %q, want %q", out.String(), payload)
	}
}
