FROM gcr.io/google_appengine/python

RUN apt-get -y update && apt-get install -y libav-tools


RUN virtualenv /env
ENV VIRTUAL_ENV /env
ENV PATH /env/bin:$PATH

ADD requirements.txt /app/requirements.txt
#ADD <service-account-key-name>.json /app/<service-account-key-name>.json
#ENV GOOGLE_APPLICATION_CREDENTIALS <service-account-key-name>.json
RUN pip3 install -r /app/requirements.txt

ADD . /app
CMD [ "python3", "worker.py" ] 