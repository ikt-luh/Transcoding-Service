FROM pyrabbit-transcoder-hw

RUN pip install --no-cache-dir fastapi uvicorn

RUN mkdir -p /media /app/src
ENV MEDIA_DIR=/media
ENV SOURCE_DIR=/app/src

COPY ./src/ /app/src/
WORKDIR /app/src

EXPOSE 8000

CMD ["uvicorn", "backend:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]