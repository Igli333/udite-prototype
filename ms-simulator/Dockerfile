FROM python:3.13.10-alpine

WORKDIR .

COPY /domain /domain
COPY /ms-simulator /ms-simulator
COPY /services /services
COPY /mock_simulation_results /mock_simulation_results
COPY app.py /app.py

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

EXPOSE 8000

CMD ["python", "app.py"]