package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"server/cluster"
	"server/config"
	"server/util"
)

// StartHTTPServer starts the http server
func StartHTTPServer(paxosServer *cluster.ServerState) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /keys/{key}", func(writer http.ResponseWriter, request *http.Request) { handleGet(writer, request, paxosServer) })
	mux.HandleFunc("PUT /keys/{key}", func(writer http.ResponseWriter, request *http.Request) { handlePost(writer, request, paxosServer) })
	mux.HandleFunc("DELETE /keys/{key}", func(writer http.ResponseWriter, request *http.Request) { handleDelete(writer, request, paxosServer) })
	mux.HandleFunc("/keys", func(writer http.ResponseWriter, request *http.Request) { listHandler(writer, request, paxosServer) })
	server := &http.Server{Addr: config.HTTPListenAddress, Handler: mux}
	go serveHTTP(server)
	return server
}

func serveHTTP(server *http.Server) {
	util.SlogPanic(server.ListenAndServe().Error())
}

// HTTP GET /list endpoint, returns a list of keys and their revisions
// Available Parameters:
// linearized - whether the call should be linearized(otherwise sequentially consistent)
// omit-deleted - whether the list should omit deleted entries(otherwise include them)
func listHandler(writer http.ResponseWriter, request *http.Request, paxosServer *cluster.ServerState) {
	slog.Info("Received HTTP GET /keys", slog.String("url", request.URL.String()))
	if request.Method != "GET" {
		writer.WriteHeader(400)
		return
	}
	linearized := request.URL.Query().Get("linearized") != ""
	omitDeleted := request.URL.Query().Get("omit-deleted") != ""
	writer.Header().Set("Content-Type", "application/json")
	_, err := fmt.Fprintf(writer, "[")
	if err != nil {
		slog.Error("Error returing key list")
		writer.WriteHeader(500)
		return
	}
	var keyrevs []*cluster.KeyRev
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

// HTTP GET /key/{key} endpoint, returns a value and a revision for a given key
// Available Parameters:
// linearized - whether the call should be linearized(otherwise sequentially consistent)
// revision-only - whether to only return the revision and not include the value
func handleGet(writer http.ResponseWriter, request *http.Request, paxosServer *cluster.ServerState) {
	slog.Info("Received HTTP GET", slog.String("url", request.URL.String()))
	key := request.PathValue("key")
	linearized := request.URL.Query().Get("linearized") != ""
	revisionOnly := request.URL.Query().Get("revision-only") != ""
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
		var response *cluster.DataEntry
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

// HTTP PUT /key/{key} endpoint, conditionally write a given key value pair into the database
// The body the request is the value to write
// Available Parameters:
// revision - if supplied, the write will only be applied if it matches the current revision of the key
// async - if supplied, this endpoint will reply immediately without feedback and the write will be performed asynchronously
func handlePost(writer http.ResponseWriter, request *http.Request, paxosServer *cluster.ServerState) {
	slog.Info("Received HTTP PUT", slog.String("url", request.URL.String()))
	body := request.Body
	value, err := io.ReadAll(body)
	if err != nil {
		writer.WriteHeader(400)
	}
	valueStr := string(value)
	handleWrite(request, paxosServer, writer, &valueStr)
}

// HTTP DELETE /key/{key} endpoint, conditionally delete a given key value pair from the database
// A deleted key is still present in the database, but with no value.
// Available Parameters:
// revision - if supplied, the delete will only be applied if it matches the current revision of the key
// async - if supplied, this endpoint will reply immediately without feedback and the delete will be performed asynchronously
func handleDelete(writer http.ResponseWriter, request *http.Request, paxosServer *cluster.ServerState) {
	slog.Info("Received HTTP DELETE", slog.String("url", request.URL.String()))
	handleWrite(request, paxosServer, writer, nil)
}

// combined implementation for the POST and DELETE endpoints
func handleWrite(request *http.Request, paxosServer *cluster.ServerState, writer http.ResponseWriter, value *string) {
	key := request.PathValue("key")
	async := request.URL.Query().Get("async") != ""
	revisionStr := request.URL.Query().Get("revision")
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
