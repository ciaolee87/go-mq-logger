package logWriter

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

// 날짜별로 큐 이름으로 저장한다.
func Write(queueName string, logData string) {
	nowPath, err := os.Getwd()
	if err != nil {
		log.Fatal("로그 입력기 생성실패", queueName)
	}

	// 저장 폴더 확인 후 로그 입력
	path := filepath.Join(nowPath, "logs", queueName)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.MkdirAll(path, 999) // 프로그램 전체 권한으로 폴더 생성
	}

	now := time.Now()
	fileName := fmt.Sprintf("%2d-%02d-%02d.log", now.Year(), now.Month(), now.Day())
	logPath := filepath.Join(path, fileName)

	var logFile = func() *os.File {
		if _, err := os.Stat(logPath); err != nil {
			file, _ := os.Create(logPath)
			return file
		} else {
			file, _ := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
			return file
		}
	}()

	logFile.WriteString(logData + "\n")
	logFile.Close()
}
