FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN useradd -m app
USER app
# exec: uvicorn становится процессом с PID 1 (нужно для py-spy)
CMD ["sh", "-c", "exec uvicorn ${APP}:app --host 0.0.0.0 --port 8000 --workers 1"]
