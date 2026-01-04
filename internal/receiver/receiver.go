package receiver

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"internal/common"
)

func SaveReceivedFile(r *http.Request) (*os.File, error) {
	contentTypeStuff := strings.Split(r.Header.Get("Content-Type"), ";")
	fmt.Printf("Content-Type in request %v\n", contentTypeStuff[0])

	if contentTypeStuff[0] != "multipart/form-data" {
		errStr := fmt.Sprintf("no idea how to handle %v further", contentTypeStuff[0])
		log.Println(errStr)
		return nil, errors.New(errStr)
	}

	multipartFile, header, err := r.FormFile("file")

	if err != nil {
		errStr := fmt.Sprintf("error while trying to read file from POST request %v", err)
		log.Println(errStr)
		return nil, errors.New(errStr)
	}

	fmt.Printf("Content-Length from request is %d, header.Size %d\n", r.ContentLength, header.Size)

	defer multipartFile.Close()

	tempFilePath := filepath.Join(common.TempPath, header.Filename)
	localFile, err := os.Create(tempFilePath)
	if err != nil {
		errStr := fmt.Sprintf("error while creating %s locally %v", header.Filename, err)
		log.Println(errStr)
		return nil, errors.New(errStr)
	}

	io.Copy(localFile, multipartFile)

	return localFile, nil
}
