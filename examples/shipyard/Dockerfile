FROM python:3

# set the working directory in the container to /app
WORKDIR /app

# add the current directory to the container as /app
ADD . /app

RUN python --version
RUN pip --version

RUN pip install --trusted-host pypi.python.org -r requirements.txt
RUN pip list

RUN ls -alR

CMD ["python", "shipyard1.py", "--function", "write_blob"]



# docker build -t cjoakim/azure-batch-shipyard1 . 
# docker image ls | grep azure-batch-shipyard1

# docker run -d -e AZURE_STORAGE_ACCOUNT=$AZURE_STORAGE_ACCOUNT -e AZURE_STORAGE_KEY=$AZURE_STORAGE_KEY cjoakim/azure-batch-shipyard1:latest

# docker run -d -e PORT=80 -p 80:3000 cjoakim/azure-batch-shipyard1:latest
# docker stop -t 2 008038664a58
# docker exec -it b23d3ce4b830 /bin/bash
#   @b23d3ce4b830:/app# curl -v http://localhost
#   @b23d3ce4b830:/app# curl -v http://localhost/env
#
# docker push cjoakim/azure-batch-shipyard1:latest
#
# docker tag cjoakim/azure-batch-shipyard1:latest cjoakimacr.azurecr.io/azure-batch-shipyard1:latest
# docker push cjoakimacr.azurecr.io/azure-batch-shipyard1:latest
#
# az acr login --name cjoakimacr
# az acr repository list --name cjoakimacr --output table
#
# shipyard jobs add --tail stderr.txt
