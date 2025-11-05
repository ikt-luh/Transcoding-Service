FROM pyrabbit-transcoder-hw

RUN pip install --no-cache-dir fastapi uvicorn

# Patch nvidia driver
#WORKDIR /app
#COPY ./patch.sh ./docker-entrypoint.sh /usr/local/bin/
#RUN chmod +x /usr/local/bin/patch.sh /usr/local/bin/docker-entrypoint.sh

RUN mkdir -p /media /app/src
ENV MEDIA_DIR=/media
ENV SOURCE_DIR=/app/src

COPY ./src/ /app/src/
WORKDIR /app/src

EXPOSE 8000

#ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

CMD ["uvicorn", "backend:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]