package main

import (
	"context"
	"log/slog"
	"net/http"

	"server/paxos"
	"server/util"
)

type HTTPServer struct{}

func SetupHTTPServer(paxosServer *paxos.PaxosServerState) {
	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) { requestHandler(paxosServer, writer, request) })
	go serverHTTP()
}

func serverHTTP() {
	util.SlogPanic(http.ListenAndServe("localhost:8080", nil).Error())
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
	key := request.Header.Get("key")
	_, linearized := request.Header["Linearized"]
	var response *string
	if linearized {
		response = paxosServer.LinearizedRead(context.Background(), key)
	} else {
		response = paxosServer.Read(context.Background(), key)
	}
	_, err := writer.Write([]byte(*response))
	if err != nil {
		slog.Error("Error in HTTP GET")
	}
}

func handlePut(request *http.Request, paxosServer *paxos.PaxosServerState, writer http.ResponseWriter) {
	_, async := request.Header["Async"]
	key := request.Header.Get("key")
	value := request.Header.Get("value")
	if async {
		paxosServer.AsyncWrite(context.Background(), key, &value)
	} else {
		paxosServer.Write(context.Background(), key, &value)
	}
	writer.WriteHeader(200)
}

func handleDelete(request *http.Request, paxosServer *paxos.PaxosServerState, writer http.ResponseWriter) {
	key := request.Header.Get("key")
	_, async := request.Header["Async"]
	if async {
		paxosServer.AsyncWrite(context.Background(), key, nil)
	} else {
		paxosServer.Write(context.Background(), key, nil)
	}
	writer.WriteHeader(200)
}
