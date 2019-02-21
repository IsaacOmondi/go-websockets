package main

import (
	"github.com/gorilla/websocket"
	"encoding/json"
	"fmt"
	"net/http"
	"github.com/satori/go.uuid"
)

type ClientManager struct {
	clients map[*Client]bool
	broadcast chan []byte
	register chan *Client
	unregister chan *Client
}

type Client struct {
	id string
	socket *websocket.Conn
	send chan []byte
}

type Message struct {
    Sender    string `json:"sender,omitempty"`
    Recipient string `json:"recipient,omitempty"`
    Content   string `json:"content,omitempty"`
}

var manager = ClientManager{
    broadcast:  make(chan []byte),
    register:   make(chan *Client),
    unregister: make(chan *Client),
    clients:    make(map[*Client]bool),
}

func (manager *ClientManager) start() {
    for {
        select {
        case conn := <-manager.register:
            manager.clients[conn] = true
            jsonMessage, _ := json.Marshal(&Message{Content: "/A new socket has connected."})
            manager.send(jsonMessage, conn)
        case conn := <-manager.unregister:
            if _, ok := manager.clients[conn]; ok {
                close(conn.send)
                delete(manager.clients, conn)
                jsonMessage, _ := json.Marshal(&Message{Content: "/A socket has disconnected."})
                manager.send(jsonMessage, conn)
            }
        case message := <-manager.broadcast:
            for conn := range manager.clients {
                select {
                case conn.send <- message:
                default:
                    close(conn.send)
                    delete(manager.clients, conn)
                }
            }
        }
    }
}

func (manager *ClientManager) send(message []byte, ignore *Client) {
	for conn := range manager.clients {
		if conn != ignore {
			conn.send <- message
		}
	}
}

// goroutine for reading the socket data
func (c *Client) read() {
	defer func() {
		manager.unregister <- c
		c.socket.Close()
	}()

	for {
		_, message, err := c.socket.ReadMessage()
		if err != nil {
			manager.unregister <- c
			c.socket.Close()
			break
		}
		jsonMessage, _ := json.Marshal(&Message{Sender: c.id, Content: string(message)})
		manager.broadcast <- jsonMessage
	}
}

// goroutine for writing data
func (c *Client) write() {
	defer func() {
		c.socket.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			if !ok {
				c.socket.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			c.socket.WriteMessage(websocket.TextMessage, message)
		}
	}
}

// main function
func main() {
	fmt.Println("Starting application...")
	go manager.start()
	http.HandleFunc("/ws", wsPage)
	http.ListenAndServe("0.0.0.0:12345", nil)
}

func wsPage(res http.ResponseWriter, req *http.Request) {
	conn, error := (&websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}).Upgrade(res, req, nil)
	if error != nil {
		http.NotFound(res, req)
		return
	}
	u, _ := uuid.NewV4()
	stringUuid := u.String()
	client := &Client{id: stringUuid, socket: conn, send: make(chan []byte)}
	fmt.Println(stringUuid)
	manager.register <- client

	go client.read()
	go client.write()
}