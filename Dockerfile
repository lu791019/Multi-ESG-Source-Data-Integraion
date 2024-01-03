FROM python:3.9.14-bullseye

WORKDIR /app 

COPY . /app 

RUN pip3 --default-timeout=100 install -r requirements.txt 

CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]
