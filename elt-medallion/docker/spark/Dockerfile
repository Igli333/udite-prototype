FROM apache/spark:3.5.1-python3

USER root

COPY docker/spark/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

RUN groupadd -g 1000 sparkuser && \
    useradd  -m -u 1000 -g 1000 -s /bin/bash sparkuser

RUN mkdir -p /opt/ivy /opt/warehouse /opt/checkpoints /opt/data && \
    chown -R 1000:1000 /opt/ivy /opt/warehouse /opt/checkpoints /opt/data

COPY app/ /opt/app

RUN chown -R 1000:1000 /opt/app

USER 1000:1000
