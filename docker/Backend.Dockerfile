FROM transcoder-base

RUN pip install --no-cache-dir fastapi uvicorn

RUN touch /app/src/server/__init__.py /app/src/transcoder/__init__.py

RUN mkdir -p /app/media /app/source
ENV MEDIA_DIR=/app/media
ENV SOURCE_DIR=/app/source

ENV PYTHONPATH="/app/src"

EXPOSE 8000
CMD ["uvicorn", "src.server.backend:app", "--host", "0.0.0.0", "--port", "8000"]