package main

import (
	"bytes"
	"fmt"
	"testing"
)

// TestCopyEncryptDecrypt verifies that encryption followed by decryption
// returns the original plaintext.
func TestCopyEncryptDecrypt(t *testing.T) {
	payload := "Foo not bar"
	src := bytes.NewReader([]byte(payload))
	dst := new(bytes.Buffer)
	key := newEncryptionKey()

	// Encrypt the payload
	_, err := copyEncrypt(key, src, dst)
	if err != nil {
		t.Error(err)
	}

	fmt.Println(len(payload))       // original length: 11
	fmt.Println(len(dst.String()))  // encrypted length: 11 + 16 (IV) = 27

	// Decrypt back
	out := new(bytes.Buffer)
	nw, err := copyDecrypt(key, dst, out)
	if err != nil {
		t.Error(err)
	}

	// Verify total bytes written (16 IV bytes + payload)
	if nw != 16+len(payload) {
		t.Errorf("expected %d bytes written, got %d", 16+len(payload), nw)
	}

	// Verify the decrypted content matches the original
	if out.String() != payload {
		t.Errorf("decryption produced wrong content: got %q, want %q", out.String(), payload)
	}
}
