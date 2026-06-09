package main

import (
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// FastAPI의 active_connections = [] 와 같은 역할입니다.
// Go에서는 여러 고루틴이 동시에 이 리스트에 접근할 수 있으므로, 안전을 위해 sync.Mutex(잠금장치)를 함께 씁니다.
var (
	clients   = make(map[*websocket.Conn]bool)
	clientsMu sync.Mutex
)

// 웹소켓 연결 설정을 위한 Upgrader (HTTP 프로토콜을 웹소켓 프로토콜로 업그레이드)
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // 모든 도메인에서의 접속을 허용 (CORS 허용)
	},
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	// 1. 일반 HTTP 연결을 웹소켓 연결로 업그레이드
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("우버그레이드 에러: %v", err)
		return
	}
	defer ws.Close()

	// 2. 새로운 클라이언트를 등록 (안전하게 Lock 사용)
	clientsMu.Lock()
	clients[ws] = true
	clientsMu.Unlock()

	log.Println("새로운 유저가 채팅방에 입장했습니다!")

	// 3. 메시지 수신 대기 루프 (FastAPI의 async def websocket_endpoint 내 while True 루프와 같습니다)
	for {
		var msg map[string]string
		// 클라이언트로부터 JSON 형태의 메시지를 읽습니다.
		err := ws.ReadJSON(&msg)
		if err != nil {
			log.Printf("연결 끊김 또는 에러: %v", err)

			// 에러 발생 시(유저가 나갔을 때) 리스트에서 제거
			clientsMu.Lock()
			delete(clients, ws)
			clientsMu.Unlock()
			break
		}

		log.Printf("받은 메시지: %v", msg)

		// 4. 받은 메시지를 현재 접속 중인 모든 클라이언트에게 브로드캐스트
		broadcast(msg)
	}
}

func broadcast(msg map[string]string) {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	// 등록된 모든 소켓을 돌며 메시지를 전송합니다.
	for client := range clients {
		err := client.WriteJSON(msg)
		if err != nil {
			log.Printf("메시지 전송 에러: %v", err)
			client.Close()
			delete(clients, client)
		}
	}
}

func main() {
	// 파일 서버 설정 (테스트용 HTML 화면을 띄우기 위함)
	http.Handle("/", http.FileServer(http.Dir("./public")))

	// 웹소켓 엔드포인트 설정 (FastAPI의 @app.websocket("/ws") 역할)
	http.HandleFunc("/ws", handleConnections)

	log.Println("Go 채팅 서버가 :8080 포트에서 시작되었습니다! 🚀")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("서버 실행 실패: ", err)
	}
}
