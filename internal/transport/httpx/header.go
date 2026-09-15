package httpx

import (
	"fmt"
	"net/http"
)

func SerializeHeader(header http.Header) (map[string]any, error) {
	data := make(map[string]any)

	for key, values := range header {
		if len(values) == 1 {
			data[key] = values[0]
		} else {
			data[key] = values
		}
	}

	return data, nil
}

func DeserializeHeader(data map[string]any) (http.Header, error) {
	header := make(http.Header)

	for key, value := range data {
		switch v := value.(type) {
		case string:
			header[key] = []string{v}

		case []any:
			values := make([]string, 0, len(v))

			for _, item := range v {
				str, ok := item.(string)
				if !ok {
					return nil, fmt.Errorf("invalid header %q", key)
				}

				values = append(values, str)
			}

			header[key] = values

		case []string:
			header[key] = v

		default:
			return nil, fmt.Errorf(
				"invalid header %q: unexpected type %T",
				key,
				value,
			)
		}
	}

	return header, nil
}
