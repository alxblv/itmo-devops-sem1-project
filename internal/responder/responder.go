package responder

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

func SendResponseToPost(w http.ResponseWriter, stats []byte) error {

	w.Header().Set("Content-Type", "application/json")
	_, err := w.Write(stats)
	if err != nil {
		errStr := fmt.Sprintf("SendResponseToPost() failed to write bytes to ResponseWriter: %v", err)
		log.Println(errStr)
		return err
	}

	return nil
}

func SendResponseToGet(w http.ResponseWriter, archive *os.File) error {

	defer archive.Close()
	defer os.Remove(archive.Name())

	// otherwise will try to read starting from the position where we finished writing, i.e. nothing
	archive.Seek(0, io.SeekStart)

	fileInfo, err := archive.Stat()
	if err != nil {
		errStr := fmt.Sprintf("SendResponseToGet() failed to get archive size: %v", err)
		log.Println(errStr)
		return err
	}

	fmt.Printf("SendResponseToGet() archive %s has size %d\n", archive.Name(), fileInfo.Size())

	bytesToSend := make([]byte, fileInfo.Size())
	nRBytes, err := archive.Read(bytesToSend)
	if err != nil {
		errStr := fmt.Sprintf("SendResponseToGet() failed to prepare bytes to send: %v", err)
		log.Println(errStr)
		return err
	}
	fmt.Printf("SendResponseToGet() read %d bytes from prepared archive %s\n", nRBytes, archive.Name())

	w.Header().Set("Content-Type", "application/octet-stream")
	nWBytes, err := w.Write(bytesToSend)
	if err != nil {
		errStr := fmt.Sprintf("SendResponseToGet() failed to write bytes to ResponseWriter: %v", err)
		log.Println(errStr)
		return err
	}

	fmt.Printf("SendResponseToGet() wrote %d bytes to http.ResponseWriter\n", nWBytes)

	return err
}
