FROM python:3.12-slim

WORKDIR /app

# Install dependency saat build supaya start container tidak perlu pip install
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
