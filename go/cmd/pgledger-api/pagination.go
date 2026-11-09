package main

import (
	"errors"
	"net/http"
	"strconv"
)

const (
	defaultPageSize = 100
	maxPageSize     = 200
)

func parsePage(request *http.Request) (limit, offset int, err error) {
	limit = defaultPageSize
	if value := request.URL.Query().Get("limit"); value != "" {
		limit, err = strconv.Atoi(value)
		if err != nil || limit < 1 || limit > maxPageSize {
			return 0, 0, errors.New("limit must be between 1 and 200")
		}
	}
	if value := request.URL.Query().Get("offset"); value != "" {
		offset, err = strconv.Atoi(value)
		if err != nil || offset < 0 {
			return 0, 0, errors.New("offset must be zero or greater")
		}
	}
	return limit, offset, nil
}
