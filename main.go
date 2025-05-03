package main

import (
	"encoding/json"
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
		return true
	},
}

type Message struct {
	SenderIP string  `json:"sender_ip"`
	Sender   string  `json:"sender,omitempty"`
	Content  string  `json:"content,omitempty"`
	Lat      float64 `json:"lat,omitempty"`
	Lng      float64 `json:"lng,omitempty"`
}

var (
	clients         = make(map[*websocket.Conn]string)
	broadcast       = make(chan Message)
	clientLocations = make(map[string]Message)
)

func main() {
	// Serve static files
	http.Handle("/leaflet/", http.StripPrefix("/leaflet/", http.FileServer(http.Dir("./leaflet"))))
	http.HandleFunc("/", serveHome)
	http.HandleFunc("/ws", handleConnections)

	go handleMessages()

	addr := ":8443"
	fmt.Printf("✅ Secure server started at https://localhost%s\n", addr)
	log.Fatal(http.ListenAndServeTLS(addr, "cert.pem", "key.pem", nil))
}

func serveHome(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "index.html")
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	ip := strings.Split(r.RemoteAddr, ":")[0]

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("❌ WebSocket upgrade failed: %v\n", err)
		return
	}
	defer ws.Close()

	log.Printf("🔌 New client connected: %s\n", ip)
	clients[ws] = ip

	// Send all existing client locations
	for _, msg := range clientLocations {
		if err := ws.WriteJSON(msg); err != nil {
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
		clientLocations[ip] = msg

		log.Println("🌍 Connected user locations:")
		for addr, location := range clientLocations {
			log.Printf("📍 %s => Lat: %.6f, Lng: %.6f\n", addr, location.Lat, location.Lng)
		}

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

