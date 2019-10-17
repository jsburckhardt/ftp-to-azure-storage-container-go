/*
Copyright © 2019 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/jlaffaye/ftp"
	"github.com/spf13/cobra"
)

var storageAccountName string
var storageAccountKey string
var storageContainer string
var ftpServer string
var ftpPort string
var ftpUsername string
var ftpPassword string
var ftpPath string

// syncCmd represents the sync command
var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Download data from FTP server to storage account",
	Long: `Download data from FTP server to storage account. For example:

	./ftptoazurestorage sync --ftppassword="<ftp password>" --ftppath="<ftp path>" --ftpport="<ftp port>" --ftpserver="<ftp server>" --ftpusername="<ftp user>" --storageaccountkey="<storage account key>" --storageaccountname="<storage account name>" --storagecontainer="<container>"
	./ftptoazurestorage sync --ftppassword "<ftp password>" --ftppath "<ftp path>" --ftpport "<ftp port>" --ftpserver "<ftp server>" --ftpusername "<ftp user>" --storageaccountkey "<storage account key>" --storageaccountname "<storage account name>" --storagecontainer "<container>"
	./ftptoazurestorage sync -p "<ftp password>" -a "<ftp path>" -t "<ftp port>" -s "<ftp server>" -u "<ftp user>" -k "<storage account key>" -n "<storage account name>" -c "<container>"`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Printf("Function triggered at %v\n", time.Now())
		// load config from env variables.
		// TODO: use viper to load config.
		// TODO: verify if port comes with or without :
		// storageAccountName, storageAccountKey, storageContainer, ftpServer, ftpPort, ftpUsername, ftpPassword, ftpPath := os.Getenv("STORAGE_ACCOUNT_NAME"), os.Getenv("STORAGE_ACCESS_KEY"), os.Getenv("STORAGE_CONTAINER"), os.Getenv("FTPSERVER"), os.Getenv("FTPPORT"), os.Getenv("FTPUSERNAME"), os.Getenv("FTPPASSWORD"), os.Getenv("FTPPATH")
		// Connecting to Azure Storage Account
		// Preparing to upload files
		// connect to storage account

		// TODO: validate parameters

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

			// generating hash to validate if file exists
			h := md5.New()
			io.WriteString(h, string(buf))
			hash := fmt.Sprintf("%x", h.Sum(nil))
			blobURL := containerURL.NewBlockBlobURL(entry.Name)

			// Checking if blob exists and retrieving the properties.
			existingBlobProperties, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{})
			if err != nil && strings.Contains(err.Error(), "BlobNotFound") {
				_, err = azblob.UploadBufferToBlockBlob(ctx, buf, blobURL, azblob.UploadToBlockBlobOptions{})
				log.Printf("Uploading file %v", entry.Name)
				if err != nil {
					log.Printf("Couldn't uploade file %v. Error: %v", entry.Name, err)
					ftpdata.Close()
					continue
				}
			} else if err != nil {
				log.Printf("Couldn't uploade file %v. Error: %v", entry.Name, err)
				ftpdata.Close()
				continue
			} else if fmt.Sprintf("%x", existingBlobProperties.ContentMD5()) == hash {
				log.Printf("File %v already in storage account", entry.Name)
				ftpdata.Close()
				continue
			} else {
				_, err = azblob.UploadBufferToBlockBlob(ctx, buf, blobURL, azblob.UploadToBlockBlobOptions{})
				log.Printf("File %v already in storage account but wrong hash... replacing", entry.Name)
				if err != nil {
					log.Printf("Couldn't uploade file %v. Error: %v", entry.Name, err)
					ftpdata.Close()
					continue
				}
			}

			_, err = azblob.UploadBufferToBlockBlob(ctx, buf, blobURL, azblob.UploadToBlockBlobOptions{})
			if err != nil {
				log.Fatalf("Doom %v", entry.Name)
			}

		}
		log.Printf("Received %v files.\n", i)
	},
}

func init() {
	rootCmd.AddCommand(syncCmd)

	syncCmd.Flags().StringVarP(&storageAccountName, "storageaccountname", "n", "storageaccountname", "storage account name")
	syncCmd.Flags().StringVarP(&storageAccountKey, "storageaccountkey", "k", "storageaccountkey", "storage account key")
	syncCmd.Flags().StringVarP(&storageContainer, "storagecontainer", "c", "storagecontainer", "storage account container")
	syncCmd.Flags().StringVarP(&ftpServer, "ftpserver", "s", "ftpserver", "ftp server")
	syncCmd.Flags().StringVarP(&ftpPort, "ftpport", "t", "21", "port of the ftp server")
	syncCmd.Flags().StringVarP(&ftpUsername, "ftpusername", "u", "ftpusername", "ftp username")
	syncCmd.Flags().StringVarP(&ftpPassword, "ftppassword", "p", "ftppassword", "ftp password")
	syncCmd.Flags().StringVarP(&ftpPath, "ftppath", "a", "/", "ftp path")

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// syncCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// syncCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
