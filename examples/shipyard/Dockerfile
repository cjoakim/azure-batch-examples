FROM python:3

# set the working directory in the container to /app
WORKDIR /app

# add the current directory to the container as /app
ADD . /app

RUN python --version
RUN pip --version

RUN pip install --trusted-host pypi.python.org -r requirements.txt
RUN pip list

# Comment-out CMD when building for Shipyard!
# CMD ["python", "shipyard1.py", "--function", "simple_etl"]

# docker build -t cjoakim/azure-batch-shipyard1 . 
# docker image ls | grep azure-batch-shipyard1
# 
# docker run -d -e AZURE_STORAGE_ACCOUNT=$AZURE_STORAGE_ACCOUNT -e AZURE_STORAGE_KEY=$AZURE_STORAGE_KEY cjoakim/azure-batch-shipyard1:latest python /app/shipyard1.py --function simple_etl

# docker run -d -e AZURE_STORAGE_ACCOUNT=$AZURE_STORAGE_ACCOUNT -e AZURE_STORAGE_KEY=$AZURE_STORAGE_KEY cjoakim/azure-batch-shipyard1:latest python /app/blob_process.py --function state_center --input_container batchin --input_blob postal_codes_ct.csv --output_container batchout --output_blob result_ct.csv --logging_container batchlog


# docker push cjoakim/azure-batch-shipyard1:latest
