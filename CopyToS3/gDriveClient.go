package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
)

const (
	credentialsFile = "/Users/axit/Downloads/gmailCredDesktop.json" // your oAuth Credentials
	tokenFile       = "/Users/axit/Desktop/GDSync/CopyToS3/token.json"
)

func GetClient() *drive.Service {
	ctx := context.Background()

	credentials, err := ioutil.ReadFile(credentialsFile)
	if err != nil {
		log.Fatalf("unable to read client secret file: %v", err)
	}

	config, err := google.ConfigFromJSON(credentials, drive.DriveScope)
	if err != nil {
		log.Fatalf("unable to parse client secret file to config: %v", err)
	}

	token, err := GetToken(config)
	if err != nil {
		log.Fatalf("unable to get token: %v", err)
	}

	// Create a Drive service client
	client := config.Client(ctx, token)
	srv, err := drive.New(client)
	if err != nil {
		log.Fatalf("Unable to retrieve Drive client: %v", err)
	}

	return srv
}

func GetToken(config *oauth2.Config) (*oauth2.Token, error) {
	// Read token from file
	token, err := TokenFromFile()
	if err == nil {
		return token, nil
	}

	// No token found, initiate OAuth2 flow
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser then type the authorization code: \n%v\n", authURL)

	var authCode string
	fmt.Print("Authorization Code: ")
	_, err = fmt.Scan(&authCode)
	if err != nil {
		return nil, fmt.Errorf("unable to read authorization code: %v", err)
	}

	// Exchange auth code for access token
	token, err = config.Exchange(context.Background(), authCode)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve token from web: %v", err)
	}

	// Save token to file
	err = SaveToken(token)
	if err != nil {
		return nil, fmt.Errorf("unable to save token: %v", err)
	}

	return token, nil
}

func TokenFromFile() (*oauth2.Token, error) {
	token := &oauth2.Token{}
	file, err := ioutil.ReadFile(tokenFile)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(file, token)
	if err != nil {
		return nil, err
	}
	return token, nil
}

func SaveToken(token *oauth2.Token) error {
	file, err := json.Marshal(token)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(tokenFile, file, 0644)
	if err != nil {
		return err
	}
	return nil
}
