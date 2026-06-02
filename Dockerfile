# ---- 构建阶段:用官方 Go 镜像编译出静态二进制 ----
FROM golang:1.23-alpine AS build
WORKDIR /src

# 先拷依赖清单,利用 Docker 层缓存(依赖没变就不重新下载)
COPY go.mod go.sum ./
RUN go mod download

# 再拷源码并编译。CGO 关掉 → 纯静态二进制,可跑在最小镜像里。
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/treehole .

# ---- 运行阶段:极小镜像,只放二进制 + 前端静态文件 ----
FROM alpine:3.20
WORKDIR /app

# 健康检查/HTTPS 需要根证书(虽然本服务暂不外连,放上更稳妥)
RUN apk add --no-cache ca-certificates

COPY --from=build /bin/treehole /app/treehole
COPY static /app/static

# Fly 会注入 PORT;本地默认 8888。EXPOSE 仅作文档说明。
ENV PORT=8080
EXPOSE 8080

CMD ["/app/treehole"]
