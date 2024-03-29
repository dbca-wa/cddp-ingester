CDDP Ingester
=============

Python script to ingest file geodatabases from the CDDP and copy spatial
layers into a PostgreSQL datadase. It relies on the CDDP GDB directory being
mounted in a location accessible by the script.

# Installation

Create a new Python 3.x virtualenv and install required libraries using `pip`:

    pip install -r requirements.txt

# Environment variables

This project uses environment variables to configure a number of required
settings. For local development, use a `.env` file in the project root directory
containing the following:

    CDDP_PATH="/path/to/cddp/data/GDB"
    DATABASE_HOST="database.hostname"
    DATABASE_USERNAME="db_username"
    DATABASE_PASSWORD="db_password"
    DATABASE_NAME="database_name"
    GEOSERVER_URL="https://geoserver.url"
    GEOSERVER_USERNAME="username"
    GEOSERVER_PASSWORD="password"
    GEOSERVER_WORKSPACE="workspace"
    GEOSERVER_DATASTORE="datastore"

# Running

With the virtualenv activated and env vars defined:

    python ingester.py

# Docker image

This project defines a couple of different Dockerfiles that can be run for
separate purposes (ingesting data, setting metadata, etc.) They each have
different system packages installed and different run commands. A future
improvement would be to consolidate them into a single Docker image.

To build a new ingester Docker image from `Dockerfile.ingester`:

    docker image build -t ghcr.io/dbca-wa/cddp-ingester -f Dockerfile.ingester .

To build a new metadata Docker image from `Dockerfile.metadata`:

    docker image build -t ghcr.io/dbca-wa/cddp-metadata -f Dockerfile.metadata .
