FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy and install requirements
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy scripts
COPY scripts/ /app/scripts/
COPY data/ /app/data/

# Run data generator
CMD ["python", "/app/scripts/generate_data.py"]
