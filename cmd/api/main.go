package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/joho/godotenv"

	"go-init/internal/transport/httpx"
)

const (
	shopifyAPIVersion = "2026-07"
	webPixelFile      = "webpixels.json"

	tokenExchangeGrantType    = "urn:ietf:params:oauth:grant-type:token-exchange"
	idTokenSubjectTokenType   = "urn:ietf:params:oauth:token-type:id_token"
	requestedOfflineTokenType = "urn:shopify:params:oauth:token-type:offline-access-token"
)

type (
	webPixel struct {
		Shop        *string `json:"shop"`
		WebPixelID  *string `json:"webPixelId"`
		AccessToken *string `json:"accessToken"`
	}

	tokenExchangeResponse struct {
		AccessToken string `json:"access_token"`
		Scope       string `json:"scope"`
		ExpiresIn   *int   `json:"expires_in,omitempty"`
	}

	shopifyGraphQLRequest struct {
		Query     *string        `json:"query"`
		Variables map[string]any `json:"variables,omitempty"`
	}

	shopifyResponse struct {
		Data struct {
			WebPixelCreate struct {
				WebPixel *struct {
					ID *string `json:"id"`
				} `json:"webPixel"`

				UserErrors []struct {
					Field   []*string `json:"field"`
					Message *string   `json:"message"`
				} `json:"userErrors"`
			} `json:"webPixelCreate"`
		} `json:"data"`

		Errors []struct {
			Message *string `json:"message"`
		} `json:"errors"`
	}
)

var webPixelMu sync.Mutex

func main() {
	_ = godotenv.Load()

	r := chi.NewRouter()
	r.Use(middleware.Logger)

	r.Get("/ping", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("Golang ping")

		_, _ = w.Write([]byte("welcome"))
	})

	r.Get("/shopify/web-pixel", createWebPixel)

	r.NotFound(func(w http.ResponseWriter, _ *http.Request) {
		httpx.JSON(w, http.StatusNotFound, map[string]string{
			"message": "page not found",
		})
	})

	if err := http.ListenAndServe(":3000", r); err != nil {
		fmt.Println("server error:", err)
	}
}

func createWebPixel(w http.ResponseWriter, r *http.Request) {
	shop := r.URL.Query().Get("shop")
	idToken := r.URL.Query().Get("id_token")

	if shop == "" || idToken == "" {
		httpx.JSON(w, http.StatusBadRequest, map[string]string{
			"message": "shop and id_token are required",
		})
		return
	}

	webPixelMu.Lock()
	defer webPixelMu.Unlock()

	// Check if this shop already has a Web Pixel saved locally.
	pixels, err := loadWebPixels()
	if err != nil {
		httpx.JSON(w, http.StatusInternalServerError, map[string]string{
			"message": "failed to load web pixel storage",
		})
		return
	}

	for _, pixel := range pixels {
		if pixel.Shop == nil {
			continue
		}

		if *pixel.Shop == shop {
			httpx.JSON(w, http.StatusOK, pixel)
			return
		}
	}

	// Step 1: exchange the session (id) token for an access token.
	accessToken, err := exchangeToken(&shop, &idToken)
	if err != nil {
		httpx.JSON(w, http.StatusBadGateway, map[string]string{
			"message": err.Error(),
		})
		return
	}

	// Step 2: create the Web Pixel on Shopify using that access token.
	// Using the shop domain as accountID here for testing purposes.
	webPixelID, err := createShopifyWebPixel(&shop, accessToken, &shop)
	if err != nil {
		httpx.JSON(w, http.StatusBadGateway, map[string]string{
			"message": err.Error(),
		})
		return
	}

	pixel := &webPixel{
		Shop:        &shop,
		WebPixelID:  webPixelID,
		AccessToken: accessToken,
	}

	// Save locally so the next request for this shop is a no-op.
	pixels = append(pixels, pixel)

	if err := saveWebPixels(pixels); err != nil {
		httpx.JSON(w, http.StatusInternalServerError, map[string]string{
			"message": "web pixel created but failed to save locally",
		})
		return
	}

	httpx.JSON(w, http.StatusCreated, pixel)
}

// exchangeToken exchanges a session (id) token for an offline access token
// using Shopify's token exchange flow.
// https://shopify.dev/docs/apps/build/authentication-authorization/access-tokens/token-exchange
func exchangeToken(shop *string, idToken *string) (*string, error) {
	clientID := os.Getenv("SHOPIFY_API_KEY")
	clientSecret := os.Getenv("SHOPIFY_API_SECRET")

	if clientID == "" || clientSecret == "" {
		return nil, fmt.Errorf(
			"missing SHOPIFY_API_KEY or SHOPIFY_API_SECRET env vars",
		)
	}

	form := url.Values{}
	form.Set("client_id", clientID)
	form.Set("client_secret", clientSecret)
	form.Set("grant_type", tokenExchangeGrantType)
	form.Set("subject_token", *idToken)
	form.Set("subject_token_type", idTokenSubjectTokenType)
	form.Set("requested_token_type", requestedOfflineTokenType)

	tokenURL := fmt.Sprintf(
		"https://%s/admin/oauth/access_token",
		*shop,
	)

	req, err := http.NewRequest(
		http.MethodPost,
		tokenURL,
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"create token exchange request: %w",
			err,
		)
	}

	req.Header.Set(
		"Content-Type",
		"application/x-www-form-urlencoded",
	)

	req.Header.Set(
		"Accept",
		"application/json",
	)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf(
			"request token exchange: %w",
			err,
		)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"token exchange failed with status %d",
			resp.StatusCode,
		)
	}

	var result tokenExchangeResponse

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf(
			"decode token exchange response: %w",
			err,
		)
	}

	if result.AccessToken == "" {
		return nil, fmt.Errorf(
			"token exchange did not return an access token",
		)
	}

	return &result.AccessToken, nil
}

func createShopifyWebPixel(
	shop *string,
	accessToken *string,
	accountID *string,
) (*string, error) {
	query := `
		mutation webPixelCreate($webPixel: WebPixelInput!) {
			webPixelCreate(webPixel: $webPixel) {
				webPixel {
					id
				}
				userErrors {
					field
					message
				}
			}
		}
	`

	settings, err := json.Marshal(map[string]string{
		"accountID": *accountID,
	})
	if err != nil {
		return nil, fmt.Errorf(
			"marshal web pixel settings: %w",
			err,
		)
	}

	payload, err := json.Marshal(shopifyGraphQLRequest{
		Query: &query,
		Variables: map[string]any{
			"webPixel": map[string]any{
				"settings": string(settings),
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf(
			"marshal graphql request: %w",
			err,
		)
	}

	graphqlURL := fmt.Sprintf(
		"https://%s/admin/api/%s/graphql.json",
		*shop,
		shopifyAPIVersion,
	)

	req, err := http.NewRequest(
		http.MethodPost,
		graphqlURL,
		bytes.NewReader(payload),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"create shopify request: %w",
			err,
		)
	}

	req.Header.Set(
		"Content-Type",
		"application/json",
	)

	req.Header.Set(
		"X-Shopify-Access-Token",
		*accessToken,
	)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf(
			"request shopify: %w",
			err,
		)
	}

	defer resp.Body.Close()

	var result shopifyResponse

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf(
			"decode shopify response: %w",
			err,
		)
	}

	if len(result.Errors) > 0 {
		return nil, fmt.Errorf(
			"shopify graphql error: %s",
			*result.Errors[0].Message,
		)
	}

	userErrors := result.Data.WebPixelCreate.UserErrors

	if len(userErrors) > 0 {
		return nil, fmt.Errorf(
			"shopify web pixel error: %s",
			*userErrors[0].Message,
		)
	}

	if result.Data.WebPixelCreate.WebPixel == nil {
		return nil, fmt.Errorf(
			"shopify did not return web pixel",
		)
	}

	return result.Data.WebPixelCreate.WebPixel.ID, nil
}

func loadWebPixels() ([]*webPixel, error) {
	data, err := os.ReadFile(webPixelFile)

	if os.IsNotExist(err) {
		return []*webPixel{}, nil
	}

	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return []*webPixel{}, nil
	}

	var pixels []*webPixel

	if err := json.Unmarshal(data, &pixels); err != nil {
		return nil, err
	}

	return pixels, nil
}

func saveWebPixels(pixels []*webPixel) error {
	data, err := json.MarshalIndent(
		pixels,
		"",
		"  ",
	)
	if err != nil {
		return err
	}

	return os.WriteFile(
		webPixelFile,
		data,
		0644,
	)
}
