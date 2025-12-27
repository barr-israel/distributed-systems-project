## Template Credited to Barakat Gadban and Itay Flam
# General
### Compile proto
```bash
cd <repo-root>/server
protoc --go_out=. --go-grpc_out=. ./<file.proto>
```

# Scooter Server
### Build docker image
To build the server (backend) image:
```bash
cd <repo-root>/server
docker build . -t scooter-server:<tag>
```

To build the SPA (angular) app image:
```bash
docker build -t spa:<tag> .
```
### Run server container
```bash
cd <repo-root>/etc/spa
docker run -p 50051:50051 --name scooter-server scooter-server:<tag>
```

### Docker compose
Change to `<repo-root>/src/docker/` directory and use the following commands:
```bash
docker-compose up -d # to start all containers and load balancer
docker-compose down # to delete alll containers
```


### etcd
Read all keys:
```bash
export ETCDCTL_API=3
etcdctl get "" --prefix --keys-only
```

Read all registered servers:
```bash
etcdctl get "/servers/" --prefix
```

### SPA
Initial Setup:
* Install node
* run `npm install @angular/cli`
* `cd` to spa directory (`cd <repo-root>/etc/spa`) and run `npm install`

To run the application in dev mode:
```bash
cd <repo-root>/etc/spa
npm start
```