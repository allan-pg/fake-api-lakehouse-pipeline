FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY fast_api_app ./fast_api_app
COPY pipeline ./pipeline
COPY utils ./utils

EXPOSE 8000

CMD ["uvicorn", "fast_api_app.app:app", "--host", "0.0.0.0", "--port", "8000"]