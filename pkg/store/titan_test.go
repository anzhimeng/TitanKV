package store

import (
	"bytes"
	"os"
	"testing"
)

func TestTitanBasic(t *testing.T) {
	dbPath := "/tmp/titankv_go_test"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	// 1. Open
	db, err := Open(dbPath, DefaultOptions())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// 2. Put
	key := []byte("hello")
	val := []byte("world")
	if err := db.Put(key, val); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 3. Get
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Errorf("Get want %s, got %s", val, got)
	}

	// 4. Delete
	if err := db.Delete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 5. Get Not Found
	_, err = db.Get(key)
	if err == nil {
		t.Error("Expect error for deleted key, got nil")
	}
}
