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

func StartHTTPServer(paxosServer *paxos.PaxosServerState) {
	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) { requestHandler(paxosServer, writer, request) })
	http.HandleFunc("/list", func(writer http.ResponseWriter, request *http.Request) { listHandler(paxosServer, writer, request) })
	go serverHTTP()
}

func serverHTTP() {
	util.SlogPanic(http.ListenAndServe(config.HTTPListenAddress, nil).Error())
}

func listHandler(paxosServer *paxos.PaxosServerState, writer http.ResponseWriter, request *http.Request) {
	if request.Method != "GET" {
		writer.WriteHeader(400)
		return
	}
	_, linearized := request.Header["Linearized"]
	_, err := fmt.Fprintf(writer, "[")
	if err != nil {
		slog.Error("Error returing key list")
		writer.WriteHeader(500)
		return
	}
	var keys []string
	if linearized {
		keys = paxosServer.ListKeysLinearized(context.Background())
	} else {
		keys = paxosServer.ListKeys(context.Background())
	}
	for i, key := range keys {
		if i == len(keys)-1 {
			_, err = fmt.Fprintf(writer, "\"%s\"]", key)
		} else {
			_, err = fmt.Fprintf(writer, "\"%s\",", key)
		}
		if err != nil {
			slog.Error("Error returing key list")
			writer.WriteHeader(500)
			return
		}
	}
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

func handleGet(request *http.Request, paxosServer *paxos.PaxosServerState, writer http.ResponseWriter) {
	slog.Info("Received HTTP GET", slog.String("Headers", fmt.Sprint(request.Header)))
	key := request.Header.Get("key")
	_, linearized := request.Header["Linearized"]
	var response *paxos.DataEntry
	if linearized {
		response = paxosServer.LinearizedRead(context.Background(), key)
	} else {
		response = paxosServer.Read(context.Background(), key)
	}
	if response != nil {
		writer.Header().Set("Content-Type", "application/json")
		_, err := fmt.Fprintf(writer, "{value:%s,revision:%d}", *response.Value, response.Revision)
		if err != nil {
			slog.Error("Error in HTTP GET")
		}
	} else {
		writer.WriteHeader(404)
	}
}

func handlePut(request *http.Request, paxosServer *paxos.PaxosServerState, writer http.ResponseWriter) {
	slog.Info("Received HTTP PUT", slog.String("Headers", fmt.Sprint(request.Header)))
	value := request.Header.Get("value")
	handlerWrite(request, paxosServer, writer, &value)
}

func handleDelete(request *http.Request, paxosServer *paxos.PaxosServerState, writer http.ResponseWriter) {
	slog.Info("Received HTTP DELETE", slog.String("Headers", fmt.Sprint(request.Header)))
	handlerWrite(request, paxosServer, writer, nil)
}

func handlerWrite(request *http.Request, paxosServer *paxos.PaxosServerState, writer http.ResponseWriter, value *string) {
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
		paxosServer.AsyncWrite(context.Background(), key, value, revision)
		writer.WriteHeader(200)
	} else {
		writer.Header().Set("Content-Type", "application/json")
		currentRevision := paxosServer.Write(context.Background(), key, value, revision)
		if revision == nil || *revision+1 == currentRevision {
			writer.WriteHeader(200)
		} else {
			writer.WriteHeader(412)
		}
		slog.Debug("", slog.Uint64("rev", currentRevision))
		_, err := fmt.Fprintf(writer, "{\"revision\":%d}", currentRevision)
		if err != nil {
			slog.Error("Error in HTTP PUT/DELETE")
		}

	}
}
