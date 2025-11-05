FROM nginx:stable-alpine

COPY src/nginx.conf.template /etc/nginx/templates/default.conf.template

EXPOSE 8081