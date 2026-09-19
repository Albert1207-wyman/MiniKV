FROM docker.m.daocloud.io/library/ubuntu:24.04

WORKDIR /app

COPY build/kv_server /app/kv_server
COPY k8s/start-minikv.sh /app/start-minikv.sh

RUN chmod +x /app/kv_server /app/start-minikv.sh

EXPOSE 9400
EXPOSE 9300/udp
EXPOSE 9301/udp
EXPOSE 9302/udp

ENTRYPOINT ["/app/start-minikv.sh"]