FROM python:3.7-slim-stretch
MAINTAINER asi@dbca.wa.gov.au
RUN apt-get update -y
RUN apt-get install --no-install-recommends -y gdal-bin proj-bin
RUN pip install --upgrade pip
WORKDIR /app
COPY ingester.py requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "ingester.py"]
