FROM python:2.7.18-slim-buster
WORKDIR /app
COPY *.py .
COPY ext .
COPY in .
COPY util .
COPY requirements.txt .

RUN pip install -r requirements.txt
RUN mkdir /out