FROM alpine:latest

RUN mkdir /opt/hyperscrape_coordinator/
COPY ./*.py /opt/hyperscrape_coordinator/
COPY ./requirements.txt /opt/hyperscrape_coordinator/requirements.txt
WORKDIR /opt/hyperscrape_coordinator/
RUN apk update
RUN apk add python3 py3-pip
RUN pip install -r ./requirements.txt --break-system-packages --root-user-action=ignore
EXPOSE 8000
CMD python main.py