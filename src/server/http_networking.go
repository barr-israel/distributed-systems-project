package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"server/config"
	"server/paxos"
	"server/util"
)

type HTTPServer struct{}

// StartHTTPServer starts the http server
func StartHTTPServer(paxosServer *paxos.PaxosServerState) {
	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) { requestHandler(paxosServer, writer, request) })
	http.HandleFunc("/list", func(writer http.ResponseWriter, request *http.Request) { listHandler(paxosServer, writer, request) })
	go serveHTTP()
}

func serveHTTP() {
	util.SlogPanic(http.ListenAndServe(config.HTTPListenAddress, nil).Error())
}

func requestHandler(paxosServer *paxos.PaxosServerState, writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case "":
		fallthrough
	case "GET":
		handleGet(request, paxosServer, writer)
	case "PUT":
		handlePut(request, paxosServer, writer)
	case "DELETE":
		handleDelete(request, paxosServer, writer)
	}
}

// HTTP GET /list endpoint, returns a list of keys and their revisions
// Available Headers:
// Linearized - whether the call should be linearized(otherwise sequentially consistent)
// Omit-Deleted - whether the list should omit deleted entries(otherwise include them)
func listHandler(paxosServer *paxos.PaxosServerState, writer http.ResponseWriter, request *http.Request) {
	slog.Info("Received HTTP GET /list", slog.String("Headers", fmt.Sprint(request.Header)))
	if request.Method != "GET" {
		writer.WriteHeader(400)
		return
	}
	_, linearized := request.Header["Linearized"]
	_, omitDeleted := request.Header["Omit-Deleted"]
	writer.Header().Set("Content-Type", "application/json")
	_, err := fmt.Fprintf(writer, "[")
	if err != nil {
		slog.Error("Error returing key list")
		writer.WriteHeader(500)
		return
	}
	var keyrevs []*paxos.KeyRev
	if linearized {
		keyrevs = paxosServer.LinearizedListKeys(context.Background(), omitDeleted)
	} else {
		keyrevs = paxosServer.ListKeys(context.Background(), omitDeleted)
	}
	for i, keyrev := range keyrevs {
		_, err = fmt.Fprintf(writer, "{\"key\":\"%s\",\"revision\":%d}", keyrev.Key, keyrev.Revision)
		if i != len(keyrevs)-1 {
			_, err = fmt.Fprintf(writer, ",")
		}
		if err != nil {
			slog.Error("Error returing key list")
			writer.WriteHeader(500)
			return
		}
	}
	_, err = fmt.Fprintf(writer, "]")
	if err != nil {
		slog.Error("Error returing key list")
		writer.WriteHeader(500)
		return
	}
}

// HTTP GET endpoint, returns a value and a revision for a given key
// Available Headers:
// key (REQUIRED) - the key of the value and revision to return
// Linearized - whether the call should be linearized(otherwise sequentially consistent)
// Revision-Only - whether to only return the revision and not include the value
func handleGet(request *http.Request, paxosServer *paxos.PaxosServerState, writer http.ResponseWriter) {
	slog.Info("Received HTTP GET", slog.String("Headers", fmt.Sprint(request.Header)))
	key := request.Header.Get("key")
	_, linearized := request.Header["Linearized"]
	_, revisionOnly := request.Header["Revision-Only"]
	ctx := request.Context()
	if revisionOnly {
		var response uint64
		if linearized {
			response = paxosServer.LinearizedReadRevision(ctx, key)
		} else {
			response = paxosServer.ReadRevision(ctx, key)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, err := fmt.Fprintf(writer, "{\"revision\":%d}", response)
		if err != nil {
			slog.Error("Error in HTTP GET")
		}
	} else {
		var response *paxos.DataEntry
		if linearized {
			response = paxosServer.LinearizedRead(ctx, key)
		} else {
			response = paxosServer.Read(ctx, key)
		}
		if response != nil {
			var err error
			writer.Header().Set("Content-Type", "application/json")
			if response.Value != nil {
				_, err = fmt.Fprintf(writer, "{\"value\":\"%s\",\"revision\":%d}", *response.Value, response.Revision)
			} else {
				_, err = fmt.Fprintf(writer, "{\"value\":null,\"revision\":%d}", response.Revision)
			}
			if err != nil {
				slog.Error("Error in HTTP GET")
			}
		} else {
			writer.WriteHeader(404)
		}
	}
}

// HTTP PUT endpoint, conditionally write a given key value pair into the database
// Available Headers:
// key (REQUIRED) - the key to write
// value (REQUIRED) - the value to write
// revision - if supplied, the write will only be applied if it matches the current revision of the key
// Async - if supplied, this endpoint will reply immediately without feedback and the write will be performed asynchronously
func handlePut(request *http.Request, paxosServer *paxos.PaxosServerState, writer http.ResponseWriter) {
	slog.Info("Received HTTP PUT", slog.String("Headers", fmt.Sprint(request.Header)))
	value := request.Header.Get("value")
	handleWrite(request, paxosServer, writer, &value)
}

// HTTP DELETE endpoint, conditionally delete a given key value pair from the database
// A deleted key is still present in the database, but with no value.
// Available Headers:
// key (REQUIRED) - the key to write
// revision - if supplied, the delete will only be applied if it matches the current revision of the key
// Async - if supplied, this endpoint will reply immediately without feedback and the delete will be performed asynchronously
func handleDelete(request *http.Request, paxosServer *paxos.PaxosServerState, writer http.ResponseWriter) {
	slog.Info("Received HTTP DELETE", slog.String("Headers", fmt.Sprint(request.Header)))
	handleWrite(request, paxosServer, writer, nil)
}

// combined implementation for the PUT and DELETE endpoints
func handleWrite(request *http.Request, paxosServer *paxos.PaxosServerState, writer http.ResponseWriter, value *string) {
	_, async := request.Header["Async"]
	key := request.Header.Get("key")
	revisionStr := request.Header.Get("revision")
	var revision *uint64 = nil
	if revisionStr != "" {
		parsed, err := strconv.ParseUint(revisionStr, 10, 64)
		if err != nil {
			writer.WriteHeader(400)
			return
		}
		revision = &parsed
	}
	if async {
		go paxosServer.Write(context.Background(), key, value, revision)
		writer.WriteHeader(200)
	} else {
		writer.Header().Set("Content-Type", "application/json")
		res := paxosServer.Write(context.Background(), key, value, revision)
		if !res.Success {
			writer.WriteHeader(412)
		}
		_, err := fmt.Fprintf(writer, "{\"success\":%t,\"revision\":%d}", res.Success, res.Revision)
		if err != nil {
			slog.Error("Error in HTTP PUT/DELETE")
		}

	}
}
