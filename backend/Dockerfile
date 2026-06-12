FROM node:18-alpine
WORKDIR /app

# Install kubectl and kind so the execute endpoint can run upgrade commands
RUN apk add --no-cache curl ca-certificates && \
    curl -Lo /usr/local/bin/kubectl \
      "https://dl.k8s.io/release/v1.31.0/bin/linux/amd64/kubectl" && \
    chmod +x /usr/local/bin/kubectl && \
    curl -Lo /usr/local/bin/kind \
      "https://kind.sigs.k8s.io/dl/v0.24.0/kind-linux-amd64" && \
    chmod +x /usr/local/bin/kind

COPY package.json .
RUN npm install
COPY server.js .
EXPOSE 3000
CMD ["node", "server.js"]
