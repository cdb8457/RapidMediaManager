FROM node:22-alpine AS builder
WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .

# Inject required environment variables for the SvelteKit static adapter
ARG VERSION="latest"
ARG BASE_PATH=""
ARG PUBLIC_API_URL=""

ENV PUBLIC_VERSION=${VERSION}
ENV PUBLIC_API_URL=${PUBLIC_API_URL}
ENV BASE_PATH=${BASE_PATH}/web

# Build the static site
RUN npm run build

FROM nginx:alpine
# Copy the built static files to Nginx's web root
COPY --from=builder /app/build /usr/share/nginx/html

# Copy a basic Nginx configuration that routes everything to index.html (SPA routing)
RUN echo 'server { \
    listen 80; \
    location / { \
    root /usr/share/nginx/html; \
    try_files $uri $uri/ /index.html; \
    } \
    }' > /etc/nginx/conf.d/default.conf

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
