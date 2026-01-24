FROM python:3.13.10

WORKDIR .

COPY /app /app
COPY app.py app.py

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

EXPOSE 5003

CMD ["python", "app.py"]