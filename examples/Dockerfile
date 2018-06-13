FROM python:3
MAINTAINER Chris Joakim

# Create app directory
RUN mkdir -p /app

WORKDIR /app

# Copy the app code/files
COPY . /app/

RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "states_local.py", "CT" ]
