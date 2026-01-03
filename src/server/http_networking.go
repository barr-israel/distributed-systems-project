package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"server/config"
	"server/paxos"
	"server/util"
)

type HTTPServer struct{}

func StartHTTPServer(paxosServer *paxos.PaxosServerState) {
	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) { requestHandler(paxosServer, writer, request) })
	go serverHTTP()
}

func serverHTTP() {
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

func handleGet(request *http.Request, paxosServer *paxos.PaxosServerState, writer http.ResponseWriter) {
	slog.Info("Received HTTP GET", slog.String("Headers", fmt.Sprint(request.Header)))
	key := request.Header.Get("key")
	_, linearized := request.Header["Linearized"]
	var response *string
	if linearized {
		response = paxosServer.LinearizedRead(context.Background(), key)
	} else {
		response = paxosServer.Read(context.Background(), key)
	}
	if response != nil {
		_, err := writer.Write([]byte(*response))
		if err != nil {
			slog.Error("Error in HTTP GET")
		}
	} else {
		writer.WriteHeader(404)
	}
}

func handlePut(request *http.Request, paxosServer *paxos.PaxosServerState, writer http.ResponseWriter) {
	slog.Info("Received HTTP PUT", slog.String("Headers", fmt.Sprint(request.Header)))
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
	slog.Info("Received HTTP DELETE", slog.String("Headers", fmt.Sprint(request.Header)))
	key := request.Header.Get("key")
	_, async := request.Header["Async"]
	if async {
		paxosServer.AsyncWrite(context.Background(), key, nil)
	} else {
		paxosServer.Write(context.Background(), key, nil)
	}
	writer.WriteHeader(200)
}
