## Server Cluster Running Instructions
Open the `./src/docker/` directory.  
Launch the containers(requires `docker` and `docker-compose` to be installed, the containers will be built automatically):
```bash
docker compose up
```

## Demo Application Running Instructions
Open the `./src/etc/crab-docs/` directory.  
Compile and run the app(requires a Rust tool-chain to be installed):
```bash
cargo run
```

## File Tree Details
Below are details about relevant files in the `./src/` directory:

├── 󰣞 src  
│   ├──  docker  
│   │   ├──  compose.yaml - docker-compose instructions to build and launch all the containers  
│   │   └──  setup_script - files for the init container  
│   │       ├──  Dockerfile - Dockerfile for the init container  
│   │       └──  etcd_setup.sh configuration settings set by the init container  
│   ├──  etc  
│   │   └──  crab-docs  
│   │       ├──  Cargo.toml - Rust project configuration  
│   │       └── 󰣞 src  
│   │           ├──  main.rs - main demo app code  
│   │           └──  networking.rs - client to use to communicate with the cluster  
│   └──  server  
│       ├──  cluster  
│       │   ├──  acceptor_requests.go - implementations of the gRPC endpoints directed at Paxos acceptors  
│       │   ├──  client_api.go - API functions that can be called to interact with the database(initiate read/write).  
│       │   ├──  common.go - the server state structure itself and common functions that uses it  
│       │   ├──  grpc.proto - gRPC structures and requests definitions.  
│       │   ├──  leader_requests.go - implementations of the gRPC endpoints not related directly to Paxos, and are directed at the cluster leader.  
│       │   ├──  paxos_acceptor.go - the state and functions implementing a Paxos acceptor for a single Paxos instance  
│       │   ├──  paxos_leader.go - implementations of the gRPC endpoints not are directly to Paxos, and are directed at the cluster leader.  
│       │   └──  setup.go - functions that are used to setup a new server.  
│       ├──  config  
│       │   └──  config.go - reads configuration from command arguments.  
│       ├──  Dockerfile - Dockerfile for the server container  
│       ├──  etcd  
│       │   └──  etcd.go - client code to interact with the etcd server  
│       ├──  httpserver  
│       │   └──  httpserver.go - sets up and serves the http server, forwards requests to functions in cluster/client_api.go  
│       ├──  server.go - the main entry point of the program that launches the database server itself and the http server that interacts with it  
│       └──  util  
│           └──  util.go - utility functions  
