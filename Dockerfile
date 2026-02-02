FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

# LLM config (Gemini) — set GEMINI_API_KEY in deployment env
ENV LLM_ENABLED=true
ENV LLM_PROVIDER=gemini
ENV LLM_MODEL=gemini-1.5-flash

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

EXPOSE 8000
CMD ["uvicorn", "service_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
