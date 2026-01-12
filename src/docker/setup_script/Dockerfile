FROM alpine:latest
RUN apk add --no-cache etcd-ctl
COPY ./etcd_setup.sh ./
ENTRYPOINT ["/bin/sh", "./etcd_setup.sh"]
