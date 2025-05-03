package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
)

type Message struct {
	SenderIP string  `json:"sender_ip"`
	Sender   string  `json:"sender,omitempty"`
	Content  string  `json:"content,omitempty"`
	Lat      float64 `json:"lat,omitempty"`
	Lng      float64 `json:"lng,omitempty"`
}

var (
	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	clients         = make(map[*websocket.Conn]string)
	broadcast       = make(chan Message)
	clientLocations = make(map[string]Message)
	messageHistory  = []Message{}

	mutex = sync.Mutex{}

	messagesFile  = "messages.json"
	locationsFile = "locations.json"
)

func main() {
	// Load stored messages and locations
	loadData()

	http.Handle("/leaflet/", http.StripPrefix("/leaflet/", http.FileServer(http.Dir("./leaflet"))))
	http.HandleFunc("/", serveHome)
	http.HandleFunc("/ws", handleConnections)

	go handleMessages()

	addr := ":8443"
	fmt.Printf("✅ Offline server running at https://localhost%s\n", addr)
	log.Fatal(http.ListenAndServeTLS(addr, "cert.pem", "key.pem", nil))
}

func serveHome(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "index.html")
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	ip := strings.Split(r.RemoteAddr, ":")[0]

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("❌ WebSocket upgrade error: %v\n", err)
		return
	}
	defer ws.Close()

	log.Printf("🔌 New client connected: %s\n", ip)
	mutex.Lock()
	clients[ws] = ip
	mutex.Unlock()

	// Send message history
	mutex.Lock()
	for _, msg := range messageHistory {
		ws.WriteJSON(msg)
	}
	mutex.Unlock()

	// Send current locations
	mutex.Lock()
	for _, loc := range clientLocations {
		ws.WriteJSON(loc)
	}
	mutex.Unlock()

	for {
		var msg Message
		err := ws.ReadJSON(&msg)
		if err != nil {
			log.Printf("⚠️ Client %s disconnected: %v\n", ip, err)
			mutex.Lock()
			delete(clients, ws)
			delete(clientLocations, ip)
			saveLocations()
			mutex.Unlock()
			break
		}

		msg.SenderIP = ip

		mutex.Lock()
		if msg.Content != "" {
			messageHistory = append(messageHistory, msg)
			saveMessages()
		}
		if msg.Lat != 0 || msg.Lng != 0 {
			clientLocations[ip] = msg
			saveLocations()
		}
		mutex.Unlock()

		broadcast <- msg
	}
}

func handleMessages() {
	for {
		msg := <-broadcast
		mutex.Lock()
		for client := range clients {
			client.WriteJSON(msg)
		}
		mutex.Unlock()
	}
}

// 🔽 Save messages to disk
func saveMessages() {
	data, _ := json.MarshalIndent(messageHistory, "", "  ")
	_ = ioutil.WriteFile(messagesFile, data, 0644)
}

// 🔽 Save locations to disk
func saveLocations() {
	data, _ := json.MarshalIndent(clientLocations, "", "  ")
	_ = ioutil.WriteFile(locationsFile, data, 0644)
}

// 🔼 Load messages and locations from disk
func loadData() {
	if data, err := ioutil.ReadFile(messagesFile); err == nil {
		_ = json.Unmarshal(data, &messageHistory)
	}
	if data, err := ioutil.ReadFile(locationsFile); err == nil {
		_ = json.Unmarshal(data, &clientLocations)
	}
}

