package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/jlaffaye/ftp"
)

func main() {
	log.Printf("Function triggered at %v\n", time.Now())
	// load config from env variables.
	// TODO: use viper to load config.
	// TODO: verify if port comes with our without :
	storageAccountName, storageAccountKey, storageContainer, ftpServer, ftpPort, ftpUsername, ftpPassword, ftpPath := os.Getenv("STORAGE_ACCOUNT_NAME"), os.Getenv("STORAGE_ACCESS_KEY"), os.Getenv("STORAGE_CONTAINER"), os.Getenv("FTPSERVER"), os.Getenv("FTPPORT"), os.Getenv("FTPUSERNAME"), os.Getenv("FTPPASSWORD"), os.Getenv("FTPPATH")

	// Connecting to Azure Storage Account
	// Preparing to upload files
	// connect to storage account
	skc, err := azblob.NewSharedKeyCredential(storageAccountName, storageAccountKey)
	if err != nil {
		log.Fatalf("Error generating key to storage account. Error: %v", err)
	}
	pipeline := azblob.NewPipeline(skc, azblob.PipelineOptions{})
	url, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", storageAccountName, storageContainer))
	containerURL := azblob.NewContainerURL(*url, pipeline)

	// connect to ftp server
	conn := ftpServer + ":" + ftpPort
	log.Printf("Connecting to ftp server %v\n", conn)
	c, err := ftp.Dial(conn, ftp.DialWithTimeout(5*time.Second))
	if err != nil {
		log.Printf("Error creating client to ftp server: %v", err)
	}

	err = c.Login(ftpUsername, ftpPassword)
	if err != nil {
		log.Printf("Authentication error: %v", err)
	}

	// get ftp files
	entries, err := c.List(ftpPath)
	if err != nil {
		log.Printf("Error while listing files: %v", err)
	}

	c.ChangeDir(ftpPath)

	// After successful connection with
	// the ftp server now we are going to
	// Create container in storage
	ctx := context.Background()
	log.Printf("Creating container %v", storageContainer)
	_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessBlob)
	if err != nil {
		if strings.Contains(err.Error(), "Code: ContainerAlreadyExists") {
			log.Printf("Container %v already exists", storageContainer)
		} else {
			log.Fatalf("Can't create container %v. Error: %v", storageContainer, err)
		}
	} else {
		log.Printf("Successfully created container: %v", storageContainer)
	}

	var i int
	var entry *ftp.Entry
	for i, entry = range entries {
		if entry.Type == ftp.EntryTypeFolder {
			// TODO recursive
			fmt.Printf("Item is a folder: %v\n", entry.Name)
			continue
		}

		ftpdata, err := c.Retr(entry.Name)
		if err != nil {
			log.Printf("Error while reading data: %v", err)
			continue
		}

		buf, err := ioutil.ReadAll(ftpdata)
		if err != nil {
			log.Printf("Error while storing data into buffer: %v", err)
			continue
		}

		ftpdata.Close()
		fmt.Printf("\rDownloading file %d/%d", i+1, len(entries))

		blobURL := containerURL.NewBlockBlobURL(entry.Name)
		_, err = azblob.UploadBufferToBlockBlob(ctx, buf, blobURL, azblob.UploadToBlockBlobOptions{})
		if err != nil {
			log.Fatalf("Doom %v", entry.Name)
		}

	}
	log.Printf("Received %v files.\n", i)
}
