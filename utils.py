from dotenv import load_dotenv
import json
import logging
import os
import requests
import subprocess
import sys


# Development environment: define variables in .env
dot_env = os.path.join(os.getcwd(), '.env')
if os.path.exists(dot_env):
    load_dotenv()


def logger_setup():
    # Set up logging in a standardised way.
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def parse_cddp(cddp_path, logger=None):
    '''This function expects the CDDP filepath to be passed in
    (e.g. /mnt/GIS-CALM/GIS1-Corporate/Data/GDB), in order to walk the path and locate
    file geodatabases for copying to the database.
    Returns a list of tuples containing (path, layer_name) pairs.
    '''
    gdb_paths = []
    for i in os.walk(cddp_path):
        if '/old/' in i[0]:  # Skip the 'old' subdirectories.
            continue
        if i[0].endswith('.gdb'):
            gdb_paths.append(i[0])

    datasets = []

    for file_gdb in gdb_paths:
        try:
            gdb_layers = subprocess.check_output('ogrinfo -ro -so -q {}'.format(file_gdb), shell=True)
        except subprocess.CalledProcessError:
            if logger:
                logger.exception('ogrinfo step failed for {}'.format(file_gdb))
            continue

        layers = gdb_layers.splitlines()

        for layer in layers:
            layer_name = layer.split()[1].decode()
            datasets.append((file_gdb, layer_name))

    return datasets


def get_auth():
    return (os.getenv('GEOSERVER_USERNAME'), os.getenv('GEOSERVER_PASSWORD'))


def get_available_layers(workspace, datastore):
    # Query a datastore to get a list of available layers for publishing. Returns a list.
    url = '{}/geoserver/rest/workspaces/{}/datastores/{}/featuretypes'.format(
        os.getenv('GEOSERVER_URL'), workspace, datastore)
    headers = {'content-type': 'application/json', 'accept': 'application/json'}
    params = {'list': 'available'}
    r = requests.get(url, auth=get_auth(), headers=headers, params=params)
    if not r.status_code == 200:
        r.raise_for_status()
    return r.json()['list']['string']


def publish_layer(workspace, datastore, layer):
    # Publish a layer from a datastore.
    url = '{}/geoserver/rest/workspaces/{}/datastores/{}/featuretypes'.format(
        os.getenv('GEOSERVER_URL'), workspace, datastore)
    headers = {'content-type': 'application/json', 'accept': 'application/json'}
    body = {'featureType': {'name': layer}}
    r = requests.post(url, auth=get_auth(), headers=headers, data=json.dumps(body))
    if not r.status_code == 201:
        r.raise_for_status()
    return r


def get_layer(workspace, layer):
    # Query a published layer endpoint, then return details on that published layer as a dictionary.
    url = '{}/geoserver/rest/workspaces/{}/layers/{}'.format(os.getenv('GEOSERVER_URL'), workspace, layer)
    r = requests.get(url, auth=get_auth())
    if not r.status_code == 200:
        r.raise_for_status()
    return r.json()


def update_layer(workspace, layer, title=None, abstract=None):
    # Update the title and/or abstract attributes for a published layer.
    # Returns the response object.
    layer_dict = get_layer(workspace, layer)
    # Get the layer resource URL.
    resource_href = layer_dict['layer']['resource']['href'].replace('http', 'https')
    r = requests.get(resource_href, auth=get_auth())
    if not r.status_code == 200:
        r.raise_for_status()
    body = r.json()
    # Update the title, then PUT to the layer resource URL.
    if title:
        body['featureType']['title'] = title
    if abstract:
        body['featureType']['abstract'] = abstract
    headers = {'content-type': 'application/json'}
    r = requests.put(resource_href, auth=get_auth(), headers=headers, data=json.dumps(body))
    if not r.status_code == 200:
        r.raise_for_status()
    return r
