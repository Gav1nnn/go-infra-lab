package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStorageHTTPHandler(t *testing.T) {
	objects := newTestObjectStore(t)
	handler := NewStorageHTTPHandler("node1", objects)

	req := httptest.NewRequest(http.MethodPut, "/objects/foo.txt?version=1", bytes.NewReader([]byte("hello")))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("have status %d want 201", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/objects/foo.txt?version=1", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("have status %d want 200", rec.Code)
	}
	if rec.Body.String() != "hello" {
		t.Fatalf("have body %s want hello", rec.Body.String())
	}
}

func TestManagerHTTPHandler(t *testing.T) {
	manager := newHTTPTestCluster(t)

	req := httptest.NewRequest(http.MethodPut, "/files/foo.txt", bytes.NewReader([]byte("hello")))
	rec := httptest.NewRecorder()
	manager.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("have status %d want 201 body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/replication/run", nil)
	rec = httptest.NewRecorder()
	manager.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("have status %d want 200 body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/files/foo.txt", nil)
	rec = httptest.NewRecorder()
	manager.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("have status %d want 200 body=%s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "hello" {
		t.Fatalf("have body %s want hello", rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/files/foo.txt/metadata", nil)
	rec = httptest.NewRecorder()
	manager.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("have status %d want 200 body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec = httptest.NewRecorder()
	manager.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("have status %d want 200 body=%s", rec.Code, rec.Body.String())
	}
}

func TestManagerHTTPHandlerNodeHeartbeat(t *testing.T) {
	objects := NewHTTPObjectStore(map[string]string{})
	service := NewManagedFileService(2, objects)
	handler := NewManagerHTTPHandler(service, objects)

	body, _ := json.Marshal(map[string]string{"addr": "http://node1:9101"})
	req := httptest.NewRequest(http.MethodPut, "/internal/nodes/node1", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("have status %d want 200 body=%s", rec.Code, rec.Body.String())
	}

	nodes, err := service.Nodes()
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Fatalf("have %d nodes want 1", len(nodes))
	}
	if nodes[0].Addr != "http://node1:9101" {
		t.Fatalf("have addr %s want http://node1:9101", nodes[0].Addr)
	}
}

type httpTestCluster struct {
	handler http.Handler
}

func newHTTPTestCluster(t *testing.T) httpTestCluster {
	t.Helper()

	nodes := map[string]string{
		"node1": "http://node1",
		"node2": "http://node2",
	}
	objectStore := NewHTTPObjectStore(nodes)
	objectStore.client.Transport = fakeHTTPTransport{
		"node1": NewStorageHTTPHandler("node1", newTestObjectStore(t)),
		"node2": NewStorageHTTPHandler("node2", newTestObjectStore(t)),
	}

	service := NewManagedFileService(2, objectStore)
	for id, addr := range nodes {
		service.RegisterNode(id, addr)
	}

	// drain one copy to prove the manager can talk to remote storage handlers.
	handler := NewManagerHTTPHandler(service)
	return httpTestCluster{handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r)
	})}
}

func TestHTTPObjectStore(t *testing.T) {
	objects := NewHTTPObjectStore(map[string]string{"node1": "http://node1"})
	objects.client.Transport = fakeHTTPTransport{
		"node1": NewStorageHTTPHandler("node1", newTestObjectStore(t)),
	}

	if _, err := objects.WriteObject("node1", "foo.txt", 1, bytes.NewReader([]byte("hello"))); err != nil {
		t.Fatal(err)
	}

	_, r, err := objects.ReadObject("node1", "foo.txt", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Fatalf("have %s want hello", data)
	}
}

type fakeHTTPTransport map[string]http.Handler

func (t fakeHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	handler := t[req.URL.Host]
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec.Result(), nil
}
