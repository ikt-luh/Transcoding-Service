FROM nginx:stable-alpine

COPY src/nginx.conf /etc/nginx/nginx.conf
COPY src/backend.py /etc/nginx/backend.py

EXPOSE 8080