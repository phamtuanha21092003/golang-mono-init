package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

type GoogleDriveStorage struct {
	CLient     *drive.Service
	HttpClient *http.Client
	FolderID   string
}

type CreateUploadSessionInput struct {
	FileName    string
	ContentType string
	FileSize    int64
}

type UploadSession struct {
	URL string `json:"url"`
}

func NewGoogleDriveStorage(ctx context.Context, credentialsFile string, folderID string) (*GoogleDriveStorage, error) {
	if credentialsFile == "" {
		return nil, fmt.Errorf("missing google drive credentials file")
	}

	if folderID == "" {
		return nil, fmt.Errorf("missing google drive folder ID")
	}

	credentials, err := os.ReadFile(credentialsFile)
	if err != nil {
		return nil, fmt.Errorf("read google drive credentials: %w", err)
	}

	config, err := google.JWTConfigFromJSON(
		credentials,
		drive.DriveScope,
	)
	if err != nil {
		return nil, fmt.Errorf("parse google drive credentials: %w", err)
	}

	httpClient := config.Client(ctx)

	client, err := drive.NewService(
		ctx,
		option.WithHTTPClient(httpClient),
		option.WithScopes(drive.DriveScope),
	)
	if err != nil {
		return nil, fmt.Errorf("create google drive client: %w", err)
	}

	return &GoogleDriveStorage{
		CLient:     client,
		HttpClient: httpClient,
		FolderID:   folderID,
	}, nil
}

func (s *GoogleDriveStorage) GetFolder(ctx context.Context) error {
	files, err := s.CLient.Files.List().
		Q(fmt.Sprintf(
			"'%s' in parents and trashed = false",
			s.FolderID,
		)).
		Fields("files(id,name,mimeType,size)").
		Context(ctx).
		Do()

	if err != nil {
		return fmt.Errorf("get google drive folder: %w", err)
	}

	for _, file := range files.Files {
		fmt.Printf(
			"ID=%s | NAME=%s | MIME=%s | SIZE=%d\n",
			file.Id,
			file.Name,
			file.MimeType,
			file.Size,
		)
	}

	return nil
}

func (s *GoogleDriveStorage) CreateUploadSession(ctx context.Context, input CreateUploadSessionInput) (*UploadSession, error) {
	if input.FileName == "" {
		return nil, fmt.Errorf("missing file name")
	}

	if input.ContentType == "" {
		input.ContentType = "application/octet-stream"
	}

	metadata := &drive.File{
		Name:     input.FileName,
		MimeType: input.ContentType,
		Parents:  []string{s.FolderID},
	}

	body, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("marshal google drive metadata: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable",
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, fmt.Errorf("create google drive upload request: %w", err)
	}

	req.Header.Set(
		"Content-Type",
		"application/json; charset=UTF-8",
	)

	req.Header.Set(
		"X-Upload-Content-Type",
		input.ContentType,
	)

	if input.FileSize > 0 {
		req.Header.Set(
			"X-Upload-Content-Length",
			fmt.Sprintf("%d", input.FileSize),
		)
	}

	resp, err := s.HttpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf(
			"create google drive upload session: %w",
			err,
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, googleapi.CheckResponse(resp)
	}

	sessionURL := resp.Header.Get("Location")
	if sessionURL == "" {
		return nil, fmt.Errorf(
			"google drive did not return upload session URL",
		)
	}

	return &UploadSession{URL: sessionURL}, nil
}
