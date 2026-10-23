#Base image: lightweight Alpine Linux with Python preinstalled
FROM python:3.11-alpine

#Show log output immediately (docker logs) and skip writing .pyc files
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

#Dependencies: install pinned versions first so Docker can cache this layer
COPY requirements-lock.txt ./
RUN pip install --no-cache-dir -r requirements-lock.txt

#Application files: the Version 1 server
COPY server.py ./

#Run as a non-root user for security
RUN addgroup -S app \
    && adduser -S -G app app \
    && chown -R app:app /app
USER app

#Exposure: the server listens on port 8080
EXPOSE 8080

#Entry point: start the server automatically.
CMD ["python", "server.py", "--host", "0.0.0.0", "--port", "8080"]