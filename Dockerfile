FROM python:3.9-slim
EXPOSE 8050
ENV PYTHONUNBUFFERED=1
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
WORKDIR /app
COPY src /app/
CMD ["gunicorn", "-b :8050",  "-t 90", "app:app"]