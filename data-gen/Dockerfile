FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy generator code
COPY . .

# Run generator by default
CMD ["python", "run_all.py"]
