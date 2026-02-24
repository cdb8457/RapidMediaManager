FROM node:24-alpine AS builder
WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .

# Build the static site
RUN npm run build

FROM nginx:alpine
# Copy the built static files to Nginx's web root
COPY --from=builder /app/build /usr/share/nginx/html

# Copy a basic Nginx configuration that routes everything to index.html (SPA routing)
RUN echo 'server { \
    listen 3000; \
    location / { \
    root /usr/share/nginx/html; \
    try_files $uri $uri/ /index.html; \
    } \
    }' > /etc/nginx/conf.d/default.conf

EXPOSE 3000
CMD ["nginx", "-g", "daemon off;"]
