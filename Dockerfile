FROM python:3.11-slim

# Чтобы pip не спрашивал глупостей
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Сначала зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Потом код
COPY . .

# Чтобы импорты src.* работали
ENV PYTHONPATH=/app

# По умолчанию контейнер просто "живет" и ждёт команд
CMD ["sh", "-c", "python -m src.db.init_db && tail -f /dev/null"]