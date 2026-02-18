# Use Python 3.11 slim image
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg \
    wget \
    curl \
    git \
    pkg-config \
    build-essential \
    libavcodec-dev \
    libavformat-dev \
    libavutil-dev \
    libswscale-dev \
    libavdevice-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install NumPy first (separate layer for better caching)
RUN pip install --no-cache-dir "numpy<2.0.0"

# Install remaining Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY config.yaml .

# Create directories
RUN mkdir -p /app/models /app/temp /app/subtitles

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV TRANSFORMERS_CACHE=/app/models
ENV HF_HOME=/app/models

# Run the application
CMD ["python", "-u", "src/main.py"]
