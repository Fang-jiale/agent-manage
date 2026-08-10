# YwMatrix 网关镜像：Node 24 原生运行 TypeScript，无需构建步骤
FROM node:24-slim

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY src/ src/
COPY static/ static/

# 附件本地存储目录（挂卷持久化：-v ywmatrix-data:/app/data）
ENV AGENT_MANAGE_ADDR=:8080 \
    AGENT_MANAGE_ATTACH_DIR=/app/data/attachments
VOLUME /app/data

EXPOSE 8080
CMD ["node", "src/gateway.ts"]
