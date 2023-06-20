package main

import (
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"google.golang.org/api/drive/v3"
)

func processFolder(srv *drive.Service, folderID string, prefix string, bucket string, profile string, region string) error {

	folder, err := srv.Files.Get(folderID).Fields("id, name").Do()
	if err != nil {
		return fmt.Errorf("failed to retrieve Google Drive folder: %v", err)
	}

	fullPath := prefix + folder.Name
	log.Printf("Processing folder: %s", fullPath)

	err = processAllFilesInFolder(srv, folderID, fullPath, bucket, profile, region)
	if err != nil {
		return fmt.Errorf("error in processFilesInFolder: %s", err)
	}

	err = processSubFolders(srv, folderID, fullPath, bucket, profile, region)
	if err != nil {
		return fmt.Errorf("error in processSubFolders: %s", err)
	}

	return nil
}

func IsFile(srv *drive.Service, folderID string, prefix string, bucket string, profile string, region string) bool {

	file, err := srv.Files.Get(folderID).Fields("id, name, mimeType").Do()
	if err == nil {
		downloadChannel := make(chan io.ReadCloser)
		log.Printf("Processing file: %s", file.Name)
		go downloadFileFromGD(srv, folderID, downloadChannel)
		go uploadToS3(bucket, file.Name, profile, region, downloadChannel)
		return file.MimeType != "application/vnd.google-apps.folder"
	}

	return file.MimeType != "application/vnd.google-apps.folder"
}

func processAllFilesInFolder(srv *drive.Service, folderID string, fullPath string, bucket string, profile string, region string) error {
	files, err := srv.Files.List().Q(fmt.Sprintf("'%s' in parents and mimeType != 'application/vnd.google-apps.folder'", folderID)).Fields("files(id, name)").Do()
	if err != nil {
		return fmt.Errorf("failed to retrieve Google Drive files in folder: %v", err)
	}

	downloadChannel := make(chan io.ReadCloser)

	for _, file := range files.Files {
		fileFullPath := fullPath + "/" + file.Name

		go downloadFileFromGD(srv, file.Id, downloadChannel)
		go uploadToS3(bucket, fileFullPath, profile, region, downloadChannel)

		log.Printf("Processing file: %s", fileFullPath)
	}

	return nil
}

func processSubFolders(srv *drive.Service, folderID string, fullPath string, bucket string, profile string, region string) error {
	subfolders, err := srv.Files.List().Q(fmt.Sprintf("'%s' in parents and mimeType = 'application/vnd.google-apps.folder'", folderID)).Fields("files(id, name)").Do()
	if err != nil {
		return fmt.Errorf("failed to retrieve Google Drive subfolders in folder: %v", err)
	}

	for _, subfolder := range subfolders.Files {
		err = processFolder(srv, subfolder.Id, fullPath+"/", bucket, profile, region)
		if err != nil {
			return fmt.Errorf("failed to process subfolder '%s': %v", subfolder.Name, err)
		}
	}
	return nil
}

func downloadFileFromGD(srv *drive.Service, fileID string, downloadChannel chan io.ReadCloser) error {

	file, err := srv.Files.Get(fileID).Fields("id, name, mimeType").Do()
	if err != nil {
		return fmt.Errorf("failed to retrieve file information: %v", err)
	}

	if file.MimeType != "application/vnd.google-apps.folder" {
		if strings.HasPrefix(file.MimeType, "application/vnd.google-apps") {
			return fmt.Errorf("skipping file: %s (not exportable)", file.Name)
		}

		resp, err := srv.Files.Get(fileID).Download()
		if err != nil {
			return fmt.Errorf("failed to download file from Google Drive: %v", err)
		}
		downloadChannel <- resp.Body
		return nil
	}

	return fmt.Errorf("file is not directly downloadable")
}

func uploadToS3(bucket string, key string, profile string, region string, fileData chan io.ReadCloser) error {

	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region: aws.String(region),
		},
		Profile: profile,
	})
	if err != nil {
		log.Printf("Failed to create AWS session: %v", err)
		return err
	}

	uploader := s3manager.NewUploader(sess)

	res, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   <-fileData,
	})
	if err != nil {
		log.Printf("Failed to upload file to S3: %v", err)
		return err
	}

	fmt.Println("Upload Compelete", res)
	return nil
}

func main() {
	srv := GetClient()

	folderId := "1SGlkxj_LOo5PLvUSNj-iqZ298DEJK-iO" // "1z-Ca1cHYmnQwmoKLOX7DApfIz6JOJvDR"
	bucket := "serverless.surveys"
	profile := "axit888"
	region := "ap-northeast-1"
	folderPath := ""

	if IsFile(srv, folderId, folderPath, bucket, profile, region) {
		fmt.Println("Given Id Is A File")
		time.Sleep(10 * time.Second)
		return
	}

	err := processFolder(srv, folderId, folderPath, bucket, profile, region)
	time.Sleep(90 * time.Second)

	if err != nil {
		fmt.Println("Data Transfer Failed", err)
	} else {
		fmt.Println("Data Transfer Completed")
	}
}
