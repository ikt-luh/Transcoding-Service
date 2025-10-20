FROM transcoder-base

RUN pip install --no-cache-dir fastapi uvicorn

RUN mkdir -p /app/media /app/src
ENV MEDIA_DIR=/app/media
ENV SOURCE_DIR=/app/src

ENV PYTHONPATH="/app/src"
COPY ./src/ /app/src/

EXPOSE 8000
CMD ["uvicorn", "src.backend:app", "--host", "0.0.0.0", "--port", "8000"]