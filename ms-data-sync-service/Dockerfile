FROM python:3.13.10

WORKDIR .

COPY /model /model
COPY /repository /repository
COPY /service /service
COPY main.py main.py

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

EXPOSE 5003

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5003"]