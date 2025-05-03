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
		return true // Allow all origins (for local testing)
	},
}

var clients = make(map[*websocket.Conn]string)     // WebSocket connections with IP
var broadcast = make(chan Message)                 // Channel for broadcasting messages
var clientLocations = make(map[string]Message)     // Latest location per IP

type Message struct {
	SenderIP string  `json:"sender_ip"`
	Content  string  `json:"content,omitempty"`
	Lat      float64 `json:"lat,omitempty"`
	Lng      float64 `json:"lng,omitempty"`
}

func main() {
	// Serve static files (e.g., Leaflet)
	fs := http.FileServer(http.Dir("./leaflet"))
	http.Handle("/leaflet/", http.StripPrefix("/leaflet/", fs))

	// Serve index.html
	http.HandleFunc("/", serveHome)

	// WebSocket endpoint
	http.HandleFunc("/ws", handleConnections)

	// Handle broadcast in a goroutine
	go handleMessages()

	addr := ":8443"
	fmt.Printf("✅ Secure server started at https://localhost%s\n", addr)
	log.Fatal(http.ListenAndServeTLS(addr, "cert.pem", "key.pem", nil))
}

func serveHome(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "index.html")
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	ip := r.RemoteAddr
	ip = strings.Split(ip, ":")[0]

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("❌ WebSocket upgrade failed: %v\n", err)
		return
	}
	defer ws.Close()

	log.Printf("🔌 New client connected: %s\n", ip)
	clients[ws] = ip

	// Send all existing client locations to new client
	for _, msg := range clientLocations {
		err := ws.WriteJSON(msg)
		if err != nil {
			log.Printf("❌ Error sending existing location to %s: %v\n", ip, err)
		}
	}

	for {
		var msg Message
		err := ws.ReadJSON(&msg)
		if err != nil {
			log.Printf("⚠️ Client %s disconnected: %v\n", ip, err)
			delete(clients, ws)
			delete(clientLocations, ip)
			break
		}

		msg.SenderIP = ip
		clientLocations[ip] = msg // Save latest location

		// Log all connected users with their locations
		log.Println("🌍 Connected user locations:")
		for ipAddr, location := range clientLocations {
			log.Printf("📍 %s => Lat: %.6f, Lng: %.6f\n", ipAddr, location.Lat, location.Lng)
		}

		// Broadcast to all clients
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

