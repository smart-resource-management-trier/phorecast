FROM python:3.12.3

LABEL authors="Paul Heisterkamp"

RUN pip install --upgrade pip

# create necessary dirs and copy application
RUN mkdir /app
WORKDIR /app

COPY . .

RUN pip install gunicorn
RUN pip install -r requirements.txt
ENTRYPOINT ["gunicorn","-c","data/server-data/gunicorn.conf.py","app:app"]