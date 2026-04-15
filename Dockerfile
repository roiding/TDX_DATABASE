FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
COPY main.py .

# 默认执行每日增量同步，可通过 docker run 覆盖命令
ENTRYPOINT ["python", "main.py"]
CMD ["daily"]
