FROM python:3.11-slim

WORKDIR /app

COPY requirements-api.txt .
RUN pip install --no-cache-dir -r requirements-api.txt

COPY mock_api.py .

EXPOSE 8080

CMD ["uvicorn", "mock_api:app", "--host", "0.0.0.0", "--port", "8080"]