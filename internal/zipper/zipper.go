package zipper

import (
	"archive/zip"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"internal/common"
)

func UnzipAndStoreCSV(localFile *os.File) ([]byte, error) {

	defer localFile.Close()
	defer os.Remove(localFile.Name())

	var unzipped []byte
	zipReader, err := zip.OpenReader(localFile.Name())

	fmt.Printf("localfile: %s\n", localFile.Name())
	if err != nil {
		errStr := fmt.Sprintf("error in zip.OpenReader() %v", err)
		log.Println(errStr)
		return nil, errors.New(errStr)
	}
	defer zipReader.Close()

	wdPath, err := os.Getwd()
	if err != nil {
		errStr := fmt.Sprintf("failed to get working directory path: %v", err)
		log.Println(errStr)
		return nil, errors.New(errStr)
	}

	fmt.Printf("Working Directory: %s\n", wdPath)

	for _, f := range zipReader.File {
		filename := filepath.Base(f.Name)
		fmt.Printf("filename: %s\n", filename)

		if filename == common.DataFileName {

			readCloser, err := f.Open()
			if err != nil {
				errStr := fmt.Sprintf("error in Open(): %v", err)
				log.Println(errStr)
				return nil, errors.New(errStr)
			}

			unzipped = make([]byte, f.FileInfo().Size())
			actuallyReadBytes, err := readCloser.Read(unzipped)
			if err != nil && err != io.EOF {
				errStr := fmt.Sprintf("error in readCloser.Read(): %v", err)
				log.Println(errStr)
				return nil, errors.New(errStr)
			}

			fmt.Printf("Read %d bytes from %s\n", actuallyReadBytes, filename)

			readCloser.Close()

			// fmt.Printf("Unzipped: %s\n", unzipped)
			break // no need to read further
		}
	}

	if len(unzipped) == 0 {
		return nil, errors.New("no data.csv in provided archive")
	}

	return unzipped, nil
}

func ZipBuiltCSV(dataFile *os.File) (*os.File, error) {

	defer dataFile.Close()
	defer os.Remove(dataFile.Name())

	archive, err := os.Create(filepath.Join(common.TempPath, common.ZipFileName))
	if err != nil {
		errStr := fmt.Sprintf("ZipBuiltCSV() failed creating an archive: %v", err)
		log.Println(errStr)
		return nil, err
	}

	zipWriter := zip.NewWriter(archive)
	defer zipWriter.Close()

	fileWriter, err := zipWriter.Create(common.DataFileName)
	if err != nil {
		errStr := fmt.Sprintf("ZipBuiltCSV() failed creating a file in the archive: %v", err)
		log.Println(errStr)
		return nil, err
	}

	dataFile.Seek(0, io.SeekStart) // otherwise will try to copy starting from the position where we finished writing, i.e. nothing

	bytesCopied, err := io.Copy(fileWriter, dataFile)
	if err != nil {
		errStr := fmt.Sprintf("ZipBuiltCSV() failed to Copy(): %v", err)
		log.Println(errStr)
		return nil, err
	}

	fmt.Printf("ZipBuiltCSV() copied %d bytes to csv-file in archive\n", bytesCopied)

	return archive, nil
}
