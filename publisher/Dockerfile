FROM golang:1.14 as builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY ./src .
RUN go build -o server .
EXPOSE 80
CMD ["/app/server"]

#FROM python:3.8
#WORKDIR /app
#COPY requirements.txt .
#RUN pip install -r requirements.txt
#COPY main.py .
#EXPOSE 80
#CMD ["python", "-u", "main.py"]
