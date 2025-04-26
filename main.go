
// main.go
package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // allow all origins (for hotspot clients)
	},
}

var clients = make(map[*websocket.Conn]bool)
var broadcast = make(chan Message)
var mutex sync.Mutex

type Message struct {
	Sender  string `json:"sender"`
	Content string `json:"content"`
}

func main() {
	http.HandleFunc("/", serveHome)
	http.HandleFunc("/ws", handleConnections)
	go handleMessages()

	port := "8080"
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Printf("⚠️ Port %s in use, trying a random available port...\n", port)
		listener, err = net.Listen("tcp", ":0") // random available port
		if err != nil {
			log.Fatalf("❌ Failed to bind to a port: %v\n", err)
		}
	}

	addr := listener.Addr().String()
	fmt.Printf("✅ Server started at http://%s\n", addr)
	log.Fatal(http.Serve(listener, nil))
}

func serveHome(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Serving index.html to", r.RemoteAddr)
	http.ServeFile(w, r, "index.html")
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("❌ WebSocket upgrade failed: %v\n", err)
		return
	}
	defer ws.Close()

	log.Printf("🔌 New client connected: %s\n", ws.RemoteAddr())

	mutex.Lock()
	clients[ws] = true
	mutex.Unlock()

	for {
		var msg Message
		err := ws.ReadJSON(&msg)
		if err != nil {
			log.Printf("⚠️ Client %s disconnected: %v\n", ws.RemoteAddr(), err)
			mutex.Lock()
			delete(clients, ws)
			mutex.Unlock()
			break
		}
		log.Printf("📩 Received message from %s: %s\n", msg.Sender, msg.Content)
		broadcast <- msg
	}
}

func handleMessages() {
	for {
		msg := <-broadcast
		mutex.Lock()
		for client := range clients {
			err := client.WriteJSON(msg)
			if err != nil {
				log.Printf("❌ Error sending to client %s: %v\n", client.RemoteAddr(), err)
				client.Close()
				delete(clients, client)
			}
		}
		mutex.Unlock()
	}
}
