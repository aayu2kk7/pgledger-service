package main

import (
	"net/http/httptest"
	"testing"
)

func TestParsePage(t *testing.T) {
	cases := []struct {
		name       string
		query      string
		wantLimit  int
		wantOffset int
		wantError  bool
	}{
		{name: "defaults", wantLimit: 100, wantOffset: 0},
		{name: "explicit page", query: "?limit=25&offset=50", wantLimit: 25, wantOffset: 50},
		{name: "limit too large", query: "?limit=201", wantError: true},
		{name: "limit zero", query: "?limit=0", wantError: true},
		{name: "negative offset", query: "?offset=-1", wantError: true},
		{name: "non-numeric limit", query: "?limit=many", wantError: true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			request := httptest.NewRequest("GET", "/v1/accounts"+testCase.query, nil)
			limit, offset, err := parsePage(request)
			if (err != nil) != testCase.wantError {
				t.Fatalf("expected error=%t, got %v", testCase.wantError, err)
			}
			if !testCase.wantError && (limit != testCase.wantLimit || offset != testCase.wantOffset) {
				t.Errorf("expected limit=%d offset=%d, got limit=%d offset=%d", testCase.wantLimit, testCase.wantOffset, limit, offset)
			}
		})
	}
}
