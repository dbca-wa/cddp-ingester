FROM python:3.7-slim-stretch
MAINTAINER asi@dbca.wa.gov.au
RUN apt-get update -y \
  && apt-get upgrade -y \
  && apt-get install --no-install-recommends -y gdal-bin proj-bin \
  && rm -rf /var/lib/apt/lists/* \
  && pip install --upgrade pip
WORKDIR /app
COPY ingester.py requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "ingester.py"]
