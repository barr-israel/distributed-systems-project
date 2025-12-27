# Step 1: Build environment
FROM node:22.13.1-alpine AS build
WORKDIR /app
COPY spa/package.json spa/package-lock.json ./
RUN npm install
COPY ./spa/ .
RUN npm run build

# Step 2: Serve environment
FROM nginx:alpine
COPY --from=build /app/dist/city-scooter /usr/share/nginx/html
COPY nginx.conf /etc/nginx/nginx.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]