FROM python:3.11-alpine

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements-lock.txt ./
RUN pip install --no-cache-dir -r requirements-lock.txt

COPY routing/ routing/
COPY management/ management/
COPY storage/ storage/
RUN addgroup -S kv && adduser -S -G kv kv && mkdir /data && chown kv:kv /data
USER kv

EXPOSE 8080
CMD ["uvicorn", "routing.app:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1", "--no-access-log"]
