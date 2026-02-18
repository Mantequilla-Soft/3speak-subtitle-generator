FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir flask==3.0.0 pymongo==4.6.1 pyyaml==6.0.1 psutil==5.9.8
COPY app.py .
COPY templates/ ./templates/
CMD ["python", "-u", "app.py"]
