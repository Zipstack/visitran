FROM node:16 AS build

LABEL maintainer="Zipstack Inc."

ENV BUILD_CONTEXT_PATH frontend
ENV REACT_APP_BACKEND_URL ""
ENV REACT_APP_SOCKET_SERVICE_BASE_URL ""

# Set the working directory inside the container
WORKDIR /app

COPY ${BUILD_CONTEXT_PATH}/ .

RUN npm cache clean --force

RUN npm install --ignore-scripts
    # Build the React app (increase heap for large bundles)
ENV NODE_OPTIONS="--max-old-space-size=4096"
RUN npm run build;



# Use Nginx as the production server
FROM nginx:1.25

ARG BACKEND_PROXY_URL=http://backend:8000

# Copy the built React app to Nginx's web server directory
COPY --from=build /app/build /usr/share/nginx/html
COPY --from=build /app/nginx.conf /etc/nginx/nginx.conf

# Replace backend URL in nginx config (default: http://backend:8000 for Docker Compose)
RUN sed -i "s|http://backend:8000|${BACKEND_PROXY_URL}|g" /etc/nginx/nginx.conf

# Expose port 80 for the Nginx server
EXPOSE 80

USER nginx
# Start Nginx when the container runs
CMD ["nginx", "-g", "daemon off;"]
