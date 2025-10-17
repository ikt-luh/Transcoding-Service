FROM nginx:stable-alpine

COPY src/server/nginx.conf /etc/nginx/nginx.conf
COPY src/server/backend.py /etc/nginx/backend.py

EXPOSE 8080