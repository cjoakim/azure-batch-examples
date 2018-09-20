FROM python:3
MAINTAINER Chris Joakim

# Create app directory
RUN mkdir -p /app

WORKDIR /app

# Copy the app code & files
COPY . /app/

RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "statecenter.py", "state_center" ]

# note: see the.dockerignore file in this directory which
# specifies what files to omit from the Docker image.

# commands to build and execute this container:
# docker build -t cjoakim/statecenter .
# docker run -e st=ct cjoakim/statecenter:latest
# docker run -e st=fl cjoakim/statecenter:latest
# docker run -e st=nc cjoakim/statecenter:latest
#
# example docker commands:
# docker ps
# docker stop -t 2 86b125ed43e5
# docker push cjoakim/statecenter:latest
# docker info cjoakim/statecenter:latest
