FROM python:3.11-slim-bookworm
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir pyspark==3.5.3
WORKDIR /opt/app
COPY build_obt.py /opt/app/build_obt.py
CMD ["python", "/opt/app/build_obt.py"]
