// // main.go
// package main
//
// import (
// 	"fmt"
// 	"log"
// 	"net/http"
// 	"sync"
//
// 	"github.com/gorilla/websocket"
// )
//
// var upgrader = websocket.Upgrader{
// 	ReadBufferSize:  1024,
// 	WriteBufferSize: 1024,
// 	CheckOrigin: func(r *http.Request) bool {
// 		return true // allow all origins (for hotspot clients)
// 	},
// }
//
// var clients = make(map[*websocket.Conn]bool)
// var broadcast = make(chan Message)
// var mutex sync.Mutex
//
// type Message struct {
// 	Sender  string `json:"sender"`
// 	Content string `json:"content"`
// }
//
// func main() {
// 	http.HandleFunc("/", serveHome)
// 	http.HandleFunc("/ws", handleConnections)
//
// 	go handleMessages()
//
// 	serverAddr := "0.0.0.0:8080"
// 	fmt.Printf("✅ Server started at http://%s\n", serverAddr)
// 	log.Fatal(http.ListenAndServe(serverAddr, nil))
// }
//
// func serveHome(w http.ResponseWriter, r *http.Request) {
// 	fmt.Println("Serving index.html to", r.RemoteAddr)
// 	http.ServeFile(w, r, "index.html")
// }
//
// func handleConnections(w http.ResponseWriter, r *http.Request) {
// 	ws, err := upgrader.Upgrade(w, r, nil)
// 	if err != nil {
// 		log.Printf("❌ WebSocket upgrade failed: %v\n", err)
// 		return
// 	}
// 	defer ws.Close()
//
// 	log.Printf("🔌 New client connected: %s\n", ws.RemoteAddr())
//
// 	mutex.Lock()
// 	clients[ws] = true
// 	mutex.Unlock()
//
// 	for {
// 		var msg Message
// 		err := ws.ReadJSON(&msg)
// 		if err != nil {
// 			log.Printf("⚠️ Client %s disconnected: %v\n", ws.RemoteAddr(), err)
// 			mutex.Lock()
// 			delete(clients, ws)
// 			mutex.Unlock()
// 			break
// 		}
// 		log.Printf("📩 Received message from %s: %s\n", msg.Sender, msg.Content)
// 		broadcast <- msg
// 	}
// }
//
// func handleMessages() {
// 	for {
// 		msg := <-broadcast
// 		mutex.Lock()
// 		for client := range clients {
// 			err := client.WriteJSON(msg)
// 			if err != nil {
// 				log.Printf("❌ Error sending to client %s: %v\n", client.RemoteAddr(), err)
// 				client.Close()
// 				delete(clients, client)
// 			}
// 		}
// 		mutex.Unlock()
// 	}
// }
//

package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"

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

var clients = make(map[*websocket.Conn]bool)
var broadcast = make(chan Message)
var mutex sync.Mutex

type Message struct {
	Sender  string  `json:"sender"`
	Content string  `json:"content,omitempty"`
	Lat     float64 `json:"lat,omitempty"`
	Lng     float64 `json:"lng,omitempty"`
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

	// HTTPS server with SSL certificates (make sure to place valid cert.pem and key.pem in the root directory)
	addr := ":8443"
	fmt.Printf("✅ Secure server started at https://localhost%s\n", addr)
	log.Fatal(http.ListenAndServeTLS(addr, "cert.pem", "key.pem", nil))
}

func serveHome(w http.ResponseWriter, r *http.Request) {
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


//
// package main
//
// import (
// 	"fmt"
// 	"log"
// 	"net/http"
// 	"strings"
// 	"github.com/gorilla/websocket"
// )
//
// var upgrader = websocket.Upgrader{
// 	ReadBufferSize:  1024,
// 	WriteBufferSize: 1024,
// 	CheckOrigin: func(r *http.Request) bool {
// 		// Allow any origin for local testing
// 		return true
// 	},
// }
//
// var clients = make(map[*websocket.Conn]string) // To store clients with their IP address
// var broadcast = make(chan Message)
//
// type Message struct {
// 	SenderIP string  `json:"sender_ip"`
// 	Content  string  `json:"content,omitempty"`
// 	Lat      float64 `json:"lat,omitempty"`
// 	Lng      float64 `json:"lng,omitempty"`
// }
//
// func main() {
// 	// Serve static files (e.g., leaflet assets)
// 	fs := http.FileServer(http.Dir("./leaflet"))
// 	http.Handle("/leaflet/", http.StripPrefix("/leaflet/", fs))
//
// 	// Serve index.html
// 	http.HandleFunc("/", serveHome)
//
// 	// WebSocket endpoint
// 	http.HandleFunc("/ws", handleConnections)
//
// 	// Handle broadcasting in a separate goroutine
// 	go handleMessages()
//
// 	addr := ":8443"
// 	fmt.Printf("✅ Secure server started at https://localhost%s\n", addr)
// 	log.Fatal(http.ListenAndServeTLS(addr, "cert.pem", "key.pem", nil))
// }
//
// func serveHome(w http.ResponseWriter, r *http.Request) {
// 	http.ServeFile(w, r, "index.html")
// }
//
// func handleConnections(w http.ResponseWriter, r *http.Request) {
// 	// Get the IP address of the client
// 	ip := r.RemoteAddr
// 	ip = strings.Split(ip, ":")[0] // Extract the IP address from the remote address
//
// 	ws, err := upgrader.Upgrade(w, r, nil)
// 	if err != nil {
// 		log.Printf("❌ WebSocket upgrade failed: %v\n", err)
// 		return
// 	}
// 	defer ws.Close()
//
// 	log.Printf("🔌 New client connected: %s\n", ip)
//
// 	clients[ws] = ip
//
// 	// Notify all clients of the new connection with the IP
// 	broadcast <- Message{SenderIP: ip}
//
// 	for {
// 		var msg Message
// 		err := ws.ReadJSON(&msg)
// 		if err != nil {
// 			log.Printf("⚠️ Client %s disconnected: %v\n", ip, err)
// 			delete(clients, ws)
// 			break
// 		}
//
// 		// Broadcast received message to all clients
// 		broadcast <- msg
// 	}
// }
//
// func handleMessages() {
// 	for {
// 		msg := <-broadcast
// 		for client := range clients {
// 			err := client.WriteJSON(msg)
// 			if err != nil {
// 				log.Printf("❌ Error sending to client %s: %v\n", client.RemoteAddr(), err)
// 				client.Close()
// 				delete(clients, client)
// 			}
// 		}
// 	}
// }
//
