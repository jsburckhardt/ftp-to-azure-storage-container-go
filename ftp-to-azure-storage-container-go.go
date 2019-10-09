package main

import (
	"fmt"
	"os"
	"time"

	"github.com/jlaffaye/ftp"
)

type FtpFile struct {
	FileName  string
	FileSize  uint64
	Timestamp uint64
	Data      []byte
}

func main() {
	fmt.Printf("Function triggered at %v\n", time.Now())
	// load config from env variables.
	// TODO: use viper to load config.
	// TODO: verify if port comes with our without :
	accountName, accountKey, ftpServer, ftpPort, ftpUsername, ftpPassword, ftpPath := os.Getenv("STORAGE_ACCOUNT_NAME"), os.Getenv("STORAGE_ACCESS_KEY"), os.Getenv("FTPSERVER"), os.Getenv("FTPPORT"), os.Getenv("FTPUSERNAME"), os.Getenv("FTPPASSWORD"), os.Getenv("FTPPATH")

	// connect to ftp server
	conn := ftpServer + ":" + ftpPort
	fmt.Printf("Connecting to ftp server %v\n", conn)
	c, err := ftp.Dial(conn, ftp.DialWithTimeout(5*time.Second))
	if err != nil {
		fmt.Printf("Error creating client to ftp server: %v", err)
	}

	err = c.Login(ftpUsername, ftpPassword)
	if err != nil {
		fmt.Printf("Authentication error: %v", err)
	}

	// get ftp files
	entries, err := c.List(ftpPath)
	if err != nil {
		fmt.Printf("Error while listing files: %v", err)
	}

	c.ChangeDir(ftpPath)

	var files []FtpFile
	for i, entry := range entries {
		if entry.Type == ftp.EntryTypeFolder {
			// TODO recursive
			continue
		}

		var tempfile FtpFile
		tempfile.FileName = entry.Name
		tempfile.FileSize = entry.Size
		tempfile.Timestamp = uint64((entry.Time).Unix())

		ftpdata, err := c.Retr(entry.Name)
		if err != nil {
			fmt.Printf("Error while reading data: %v", err)
			continue
		}
		buf := make([]byte, entry.Size)
		_, err = ftpdata.Read(buf)
		if err != nil {
			fmt.Printf("Error while storing data into buffer: %v", err)
			continue
		}

		tempfile.Data = buf
		files = append(files, tempfile)
		ftpdata.Close()
		fmt.Printf("\rOn %d/%d", i, len(entries))
		//fmt.Printf("%v. Got file %v\n", i+1, tempfile.FileName)
	}
	fmt.Printf("\nReceived %v files.\n", len(files))
	fmt.Printf("hello, %v, %v", accountName, accountKey)

	// upload files to storage account

	// connect to storage account

	// verify container

	// upload file

	// store some metadata somewhere
}
