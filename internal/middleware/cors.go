package middleware

import (
	"net/http"
)

var (
	corsAllowedMethods = "OPTIONS, GET, PUT, PATCH, POST, DELETE"
	corsAllowedHeaders = "Origin, X-Requested-With, Content-Type, Accept, Authorization, authentication"
)

func CorsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		w.Header().Set("Access-Control-Allow-Methods", corsAllowedMethods)

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
