#Dockerfle for the postgres extractor container
FROM python:latest
WORKDIR /src
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY postgres_json_export.py postgres_json_export.py
CMD ["python", "-u", "postgres_json_export.py"]