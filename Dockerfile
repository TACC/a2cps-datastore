FROM python:3.9-slim
EXPOSE 8050
ENV PYTHONUNBUFFERED=TRUE
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY src /app
WORKDIR /app
CMD ["gunicorn", "-b :8050",  "-t 90", "app:app"]
