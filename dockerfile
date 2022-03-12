FROM python:latest
WORKDIR /usr/app/src
COPY . .
RUN pip3 install -r requirements.txt
CMD ["python3", "-B", "./main.py"]