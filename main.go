package main

import (
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// 웹소켓 업그레이더 설정
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // 모든 도메인에서의 접속을 허용 (테스트용)
	},
}

// ==========================================
// 1. 구조체 정의 (구조의 뼈대)
// ==========================================

// Client: 접속한 개별 유저
type Client struct {
	socket *websocket.Conn // 유저의 진짜 웹소켓 주소
	send   chan []byte     // 유저의 개인 화면으로 보낼 메시지가 대기하는 전용 우체통 (Buffer 역할)
	room   *Room           // 이 유저가 속한 방의 주소
}

// Room: 독립된 하나의 대화방
type Room struct {
	forward chan []byte      // 방 안의 모든 유저에게 뿌릴 메시지가 모이는 전광판
	join    chan *Client     // 입장하려는 유저들이 서는 대기선 (대기표)
	leave   chan *Client     // 나가려는 유저들이 서는 대기선
	clients map[*Client]bool // 현재 방에 출석한 유저들의 장부
}

// Hub: 서버의 모든 방을 총괄하는 메인 관리소
type Hub struct {
	rooms map[string]*Room // Key: 방 이름, Value: 방 객체의 주소
	mu    sync.Mutex       // 방을 새로 만들거나 찾을 때 쓸 안전 자물쇠
}

// ==========================================
// 2. 비즈니스 로직 및 생성자
// ==========================================

// NewHub: 메인 관리소를 처음 켜는 생성자 함수
func NewHub() *Hub {
	return &Hub{
		rooms: make(map[string]*Room),
	}
}

// NewRoom: 새로운 대화방을 만드는 생성자 함수
func NewRoom() *Room {
	return &Room{
		forward: make(chan []byte),
		join:    make(chan *Client),
		leave:   make(chan *Client),
		clients: make(map[*Client]bool),
	}
}

// Room의 관리자 함수: 대기표를 하나씩 뽑아서 장부를 관리하고 전광판을 돌립니다.
func (r *Room) run() {
	for {
		// 파이프라인 앞에서 join,leave,forward 중 하나라도 이벤트가 발생하면 그걸 처리하러 달려갑니다.
		select {
		case client := <-r.join:
			r.clients[client] = true
			log.Println("유저가 방에 입장했습니다.")

		case client := <-r.leave:
			if _, ok := r.clients[client]; ok {
				delete(r.clients, client)
				close(client.send)
				log.Println("유저가 방에서 퇴장했습니다.")
			}
		case msg := <-r.forward:
			for client := range r.clients {
				select {
				case client.send <- msg:
				default:
					delete(r.clients, client)
					close(client.send)
				}
			}
		}
	}
}

// Client의 읽기 비서: 브라우저가 보낸 메시지를 읽어서 '방 전광판'에 밀어 넣습니다.
func (c *Client) read() {
	defer func() {
		c.room.leave <- c // 에러가 나서 함수가 끝나면 퇴장 대기선에 서기
		c.socket.Close()
	}()
	for {
		_, msg, err := c.socket.ReadMessage()
		if err != nil {
			break // 브라우저가 닫히면 무한루프 탈출
		}
		// 읽어온 메시지를 방의 공용 전광판(forward)으로 다이렉트 전송! 줄 서세요!
		c.room.forward <- msg
	}
}

// Client의 쓰기 비서: 자기 우체통(send)에 메시지가 떨어지면 그걸 진짜 웹소켓으로 쏴서 브라우저에 그려줍니다.
func (c *Client) write() {
	defer c.socket.Close()
	for msg := range c.send { // send 채널에 물건이 들어올 때까지 잠자며 대기
		err := c.socket.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			break
		}
	}
}

// ==========================================
// 3. HTTP 웹소켓 핸들러 및 메인 함수
// ==========================================

var hub = NewHub() // 메인 허브 생성

func handleConnections(w http.ResponseWriter, r *http.Request) {
	// URL 쿼리 스트링에서 방 이름 가져오기 (예: /ws?room=apple)
	roomName := r.URL.Query().Get("room")
	if roomName == "" {
		roomName = "default" // 방 이름 없으면 기본방으로
	}

	// 1. 일반 HTTP 연결을 웹소켓으로 업그레이드
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Fatal(err)
	}

	// 2. 허브에서 방 찾기 (없으면 새로 만들기)
	hub.mu.Lock()
	room, exists := hub.rooms[roomName]
	if !exists {
		room = NewRoom()
		hub.rooms[roomName] = room
		go room.run() // 💡 중요: 새로운 방이 생기면, 방 관리자를 백그라운드 스레드(고루틴)로 즉시 가동!
		log.Printf("[%s] 방이 새로 개설되었습니다!", roomName)
	}
	hub.mu.Unlock()

	// 3. 접속한 유저(Client) 객체 조립하기
	client := &Client{
		socket: ws,
		send:   make(chan []byte, 256), // 256개까지 대기표를 쌓을 수 있는 우체통 버퍼
		room:   room,
	}

	// 4. 방의 입장 대기선(join)에 이 유저를 툭 던져두기 (줄 서기)
	room.join <- client

	// 5. 이 유저만을 위한 전용 비서 2명을 백그라운드에 배치하기
	go client.write() // 쓰기 전용 비서 출발
	client.read()     // 읽기 전용 비서는 메인 스레드에서 대기 (브라우저가 꺼질 때까지 함수를 붙잡아둠)
}

func main() {
	http.HandleFunc("/ws", handleConnections)

	log.Println("2단계 멀티룸 웹소켓 서버가 8080 포트에서 시작되었습니다...")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("서버 가동 실패: ", err)
	}
}
