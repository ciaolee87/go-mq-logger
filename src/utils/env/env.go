package env

import (
	"github.com/joho/godotenv"
	"log"
	"os"
	"path/filepath"
)

func init() {
	// 실행환경설정
	//env := flag.String("env","", "환경변수")
	//flag.Parse()
	//if flag.NFlag() == 0 {
	//	flag.Usage()
	//	log.Fatal("환경 변수를 지정하여 주십시오")
	//	return
	//}
	//
	//err := os.Setenv("GO_ENV", *env)
	//if err != nil {
	//	log.Fatal("환경 변수 로딩에 실패하였습니다")
	//}

	// 환경변수 로딩
	LoadEnv()
}

func LoadEnv() {
	getPath := func() string {
		envName := os.Getenv("GO_ENV")

		log.Println("GO_ENV", envName)
		if envName == "" {
			return ".env"
		} else {
			return ".env." + envName
		}
	}

	execPath, err := os.Getwd()
	if err != nil {
		log.Fatal("! Env. Get exec path error!")
	}

	path := filepath.Join(execPath, "env", getPath())
	log.Println("envPath", path)
	errEnvLoad := godotenv.Load(path)
	if errEnvLoad != nil {
		log.Fatal("! Error Loading DotEnv")
	}

	for _, pair := range os.Environ() {
		log.Println(pair)
	}
}

func Get(key string) string {
	return os.Getenv(key)
}
