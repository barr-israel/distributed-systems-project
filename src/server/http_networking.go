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
		keyrevs = paxosServer.ListKeysLinearized(context.Background(), omitDeleted)
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

func handlePut(request *http.Request, paxosServer *paxos.PaxosServerState, writer http.ResponseWriter) {
	slog.Info("Received HTTP PUT", slog.String("Headers", fmt.Sprint(request.Header)))
	value := request.Header.Get("value")
	handleWrite(request, paxosServer, writer, &value)
}

func handleDelete(request *http.Request, paxosServer *paxos.PaxosServerState, writer http.ResponseWriter) {
	slog.Info("Received HTTP DELETE", slog.String("Headers", fmt.Sprint(request.Header)))
	handleWrite(request, paxosServer, writer, nil)
}

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
