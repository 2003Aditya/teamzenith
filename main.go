package main

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// Allow any origin for local testing
		return true
	},
}

var clients = make(map[*websocket.Conn]string) // To store clients with their IP address
var broadcast = make(chan Message)

type Message struct {
	SenderIP string  `json:"sender_ip"`
	Content  string  `json:"content,omitempty"`
	Lat      float64 `json:"lat,omitempty"`
	Lng      float64 `json:"lng,omitempty"`
}

func main() {
	// Serve static files (e.g., leaflet assets)
	fs := http.FileServer(http.Dir("./leaflet"))
	http.Handle("/leaflet/", http.StripPrefix("/leaflet/", fs))

	// Serve index.html
	http.HandleFunc("/", serveHome)

	// WebSocket endpoint
	http.HandleFunc("/ws", handleConnections)

	// Handle broadcasting in a separate goroutine
	go handleMessages()

	addr := ":8443"
	fmt.Printf("✅ Secure server started at https://localhost%s\n", addr)
	log.Fatal(http.ListenAndServeTLS(addr, "cert.pem", "key.pem", nil))
}

func serveHome(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "index.html")
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	// Get the IP address of the client
	ip := r.RemoteAddr
	ip = strings.Split(ip, ":")[0] // Extract the IP address from the remote address

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("❌ WebSocket upgrade failed: %v\n", err)
		return
	}
	defer ws.Close()

	log.Printf("🔌 New client connected: %s\n", ip)

	clients[ws] = ip

	// Notify all clients of the new connection with the IP
	broadcast <- Message{SenderIP: ip}

	for {
		var msg Message
		err := ws.ReadJSON(&msg)
		if err != nil {
			log.Printf("⚠️ Client %s disconnected: %v\n", ip, err)
			delete(clients, ws)
			break
		}
        msg.SenderIP = ip

		// Broadcast received message to all clients
		broadcast <- msg
	}
}

func handleMessages() {
	for {
		msg := <-broadcast
		for client := range clients {
			err := client.WriteJSON(msg)
			if err != nil {
				log.Printf("❌ Error sending to client %s: %v\n", client.RemoteAddr(), err)
				client.Close()
				delete(clients, client)
			}
		}
	}
}

