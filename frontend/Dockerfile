# Multi-stage build:
#   Stage 1 uses Node to compile React into static HTML/JS/CSS.
#   Stage 2 throws Node away and serves those static files with a tiny nginx.
# The final image contains NO Node/npm — just nginx + your built files (~50MB).

# ---- Stage 1: build the static site ----
FROM node:20-alpine AS build
WORKDIR /app
# Copy manifest(s) first so `npm install` is cached unless deps change.
COPY package.json package-lock.json* ./
RUN npm install
# Now copy the rest of the source and build.
COPY . .
RUN npm run build          # outputs static files into /app/dist

# ---- Stage 2: serve with nginx ----
FROM nginx:1.27-alpine
# Our nginx config: serve the SPA + reverse-proxy /api to the backend.
COPY nginx.conf /etc/nginx/conf.d/default.conf
# Copy ONLY the built output from stage 1 (not the Node stuff).
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
