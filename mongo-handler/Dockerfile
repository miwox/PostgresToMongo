#Dockerfle for the mongoqueries container
FROM python:latest
WORKDIR /src
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY  mongoQueries.py mongoQueries.py
CMD ["python", "-u", "mongoQueries.py"]