package main

import (
	"encoding/json"
	"net/http"
)

// 1. 데이터 구조 정의 (파이썬의 Pydantic 역할)
type User struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

// 2. API 핸들러 함수
func createUserHandler(w http.ResponseWriter, r *http.Request) {
	// 오직 POST 요청만 받기
	if r.Method != http.MethodPost {
		http.Error(w, "지원하지 않는 메서드입니다.", http.StatusMethodNotAllowed)
		return
	}

	// 브라우저가 보낸 JSON 데이터를 User 구조체로 조립하기
	var user User
	json.NewDecoder(r.Body).Decode(&user)

	// 응답 보낼 데이터 만들기
	response := map[string]string{
		"message": user.Name + "님 환영합니다!",
		"status":  "success",
	}

	// JSON으로 변환해서 브라우저에 쏴주기
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// func main() {
// 	// URL 경로와 핸들러 함수 연결
// 	http.HandleFunc("/user", createUserHandler)

// 	// 8080 포트에서 서버 시작
// 	http.ListenAndServe(":8080", nil)
// }
