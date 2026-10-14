package controllers

import (
	"encoding/gob"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/sessions"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func resetWebSocketClients(t *testing.T) {
	t.Helper()
	clientsMu.Lock()
	prev := clients
	clients = make(map[*websocket.Conn]string)
	clientsMu.Unlock()
	t.Cleanup(func() {
		clientsMu.Lock()
		for c := range clients {
			_ = c.Close()
		}
		clients = prev
		clientsMu.Unlock()
	})
}

func Test_deliverJobStatus_OnlyMatchingClientReceivesEvent(t *testing.T) {
	resetWebSocketClients(t)

	up := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		id := r.URL.Query().Get("id")
		clientsMu.Lock()
		clients[ws] = id
		clientsMu.Unlock()
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				clientsMu.Lock()
				delete(clients, ws)
				clientsMu.Unlock()
				_ = ws.Close()
				return
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	connA, _, err := websocket.DefaultDialer.Dial(wsURL+"?id=user-a", nil)
	require.NoError(t, err)
	defer connA.Close()

	connB, _, err := websocket.DefaultDialer.Dial(wsURL+"?id=user-b", nil)
	require.NoError(t, err)
	defer connB.Close()

	// Give the server goroutines time to register both clients.
	require.Eventually(t, func() bool {
		clientsMu.RLock()
		defer clientsMu.RUnlock()
		return len(clients) == 2
	}, time.Second, 10*time.Millisecond)

	deliverJobStatus(JobStatus{
		Job:      "Add Task",
		Status:   "success",
		ClientID: "user-a",
	})

	_ = connA.SetReadDeadline(time.Now().Add(2 * time.Second))
	var got JobStatus
	require.NoError(t, connA.ReadJSON(&got))
	assert.Equal(t, "Add Task", got.Job)
	assert.Equal(t, "success", got.Status)
	assert.Equal(t, "user-a", got.ClientID)

	_ = connB.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	err = connB.ReadJSON(&got)
	assert.Error(t, err, "user-b must not receive user-a's job status")
}

func Test_AuthenticatedWebSocketHandler_UsesSessionUUIDNotQueryClientID(t *testing.T) {
	resetWebSocketClients(t)

	gob.Register(map[string]interface{}{})
	store := sessions.NewCookieStore([]byte("test-session-key-32bytes-long!!"))
	handler := AuthenticatedWebSocketHandler(store)

	// Establish a session cookie the same way the OAuth callback would.
	seedReq := httptest.NewRequest(http.MethodGet, "/", nil)
	seedRec := httptest.NewRecorder()
	session, err := store.Get(seedReq, "session-name")
	require.NoError(t, err)
	session.Values["user"] = map[string]interface{}{
		"email": "a@example.com",
		"id":    "1",
		"uuid":  "session-uuid-a",
	}
	require.NoError(t, session.Save(seedReq, seedRec))
	setCookie := seedRec.Result().Header.Get("Set-Cookie")
	require.NotEmpty(t, setCookie)

	server := httptest.NewServer(handler)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	header := http.Header{}
	header.Add("Cookie", setCookie)
	// Query claims a different user; session UUID must win.
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL+"?clientID=attacker-uuid", header)
	require.NoError(t, err)
	defer conn.Close()
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)

	require.Eventually(t, func() bool {
		clientsMu.RLock()
		defer clientsMu.RUnlock()
		for _, id := range clients {
			if id == "session-uuid-a" {
				return true
			}
		}
		return false
	}, time.Second, 10*time.Millisecond)

	clientsMu.RLock()
	defer clientsMu.RUnlock()
	for _, id := range clients {
		assert.Equal(t, "session-uuid-a", id)
		assert.NotEqual(t, "attacker-uuid", id)
	}
}

func Test_AddJob_IncludesClientIDInBroadcast(t *testing.T) {
	resetWebSocketClients(t)

	received := make(chan JobStatus, 16)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case status := <-broadcast:
				select {
				case received <- status:
				default:
				}
			case <-stop:
				return
			}
		}
	}()
	defer func() {
		close(stop)
		<-done
	}()

	q := NewJobQueue()
	q.AddJob(Job{
		Name:     "Add Task",
		ClientID: "user-a",
		Execute:  func() error { return nil },
	})

	var sawQueued, sawSuccess bool
	timeout := time.After(2 * time.Second)
	for !(sawQueued && sawSuccess) {
		select {
		case status := <-received:
			// Other controller tests may also emit on the shared broadcast channel.
			if status.ClientID != "user-a" || status.Job != "Add Task" {
				continue
			}
			if status.Status == "queued" {
				sawQueued = true
			}
			if status.Status == "success" {
				sawSuccess = true
			}
		case <-timeout:
			t.Fatal("timed out waiting for job status events with ClientID")
		}
	}
}
