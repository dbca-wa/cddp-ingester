from dotenv import load_dotenv
import json
import logging
import os
import requests
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET


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


def parse_cddp_qmls(cddp_path, logger=None):
    """This function expects the CDDP filepath to be passed in
    (e.g. /mnt/GIS-CALM/GIS1-Corporate/Data/GDB), in order to walk the path and locate
    QML style definitions.
    Returns a list of tuples containing (fgdb_path, layer, qml_path) triplets.
    """
    # First, get fGDB layers.
    datasets = parse_cddp(cddp_path, logger)
    qml_paths = []
    for fgdb_path, layer in datasets:
        qml_path = os.path.join(os.path.split(fgdb_path)[0], '{}.qml'.format(layer))
        if os.path.exists(qml_path):
            qml_paths.append((fgdb_path, layer, qml_path))

    return qml_paths


def get_auth():
    return (os.getenv('GEOSERVER_USERNAME'), os.getenv('GEOSERVER_PASSWORD'))


def get_available_featuretypes(workspace, datastore):
    # Query a datastore to get a list of available featuretypes for publishing. Returns a list.
    url = '{}/geoserver/rest/workspaces/{}/datastores/{}/featuretypes'.format(
        os.getenv('GEOSERVER_URL'), workspace, datastore)
    headers = {'content-type': 'application/json', 'accept': 'application/json'}
    params = {'list': 'available'}
    r = requests.get(url, auth=get_auth(), headers=headers, params=params)
    if not r.status_code == 200:
        r.raise_for_status()
    return r.json()['list']['string']


def publish_featuretype(workspace, datastore, layer):
    # Publish a layer from a datastore.
    url = '{}/geoserver/rest/workspaces/{}/datastores/{}/featuretypes'.format(
        os.getenv('GEOSERVER_URL'), workspace, datastore)
    headers = {'content-type': 'application/json', 'accept': 'application/json'}
    body = {'featureType': {'name': layer}}
    r = requests.post(url, auth=get_auth(), headers=headers, data=json.dumps(body))
    if not r.status_code == 201:
        r.raise_for_status()
    return r


def delete_featuretype(workspace, datastore, layer):
    # Delete a featuretype from a datastore.
    url = '{}/geoserver/rest/workspaces/{}/datastores/{}/featuretypes/{}'.format(
        os.getenv('GEOSERVER_URL'), workspace, datastore, layer)
    headers = {'content-type': 'application/json', 'accept': 'application/json'}
    params = {'recurse': 'true'}  # Also delete any layers associated with the featuretype.
    r = requests.get(url, auth=get_auth(), headers=headers, params=params)
    if not r.status_code == 200:
        r.raise_for_status()
    return r


def get_layers(workspace):
    # Query a workspace endpoint, then return a dict of published layers and their URLs.
    url = '{}/geoserver/rest/workspaces/{}/layers'.format(os.getenv('GEOSERVER_URL'), workspace)
    r = requests.get(url, auth=get_auth())
    if not r.status_code == 200:
        r.raise_for_status()
    layers_list = r.json()['layers']['layer']
    return {i['name']: i['href'] for i in layers_list}


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


def create_style(workspace, style, sld_string):
    # First, check if the style already exists.
    url = '{}/geoserver/rest/workspaces/{}/styles/{}.json'.format(os.getenv('GEOSERVER_URL'), workspace, style)
    r = requests.get(url, auth=get_auth())
    if r.status_code == 404:
        create = True
        # Create a new style in a workspace from an SLD XML string.
        url = '{}/geoserver/rest/workspaces/{}/styles'.format(os.getenv('GEOSERVER_URL'), workspace)
    else:
        create = False
        url = '{}/geoserver/rest/workspaces/{}/styles/{}'.format(os.getenv('GEOSERVER_URL'), workspace, style)
    headers = {'content-type': 'application/vnd.ogc.se+xml', 'accept': 'application/json'}
    if create:  # Create style.
        r = requests.post(url, auth=get_auth(), headers=headers, data=sld_string)
    else:  # Update style.
        r = requests.put(url, auth=get_auth(), headers=headers, data=sld_string)
    return r


def set_layer_style(workspace, layer):
    # Assumes that the layer and style have identical names.
    # First, get the layer details:
    url = '{}/geoserver/rest/workspaces/{}/layers/{}'.format(os.getenv('GEOSERVER_URL'), workspace, layer)
    r = requests.get(url, auth=get_auth())
    if not r.status_code == 200:
        r.raise_for_status()
    style_href = '{}/geoserver/rest/workspaces/{}/styles/{}.json'.format(os.getenv('GEOSERVER_URL'), workspace, layer)
    d = r.json()
    # Set the layer's default style.
    d['layer']['defaultStyle'] = {'name': layer, 'href': style_href}
    headers = {'content-type': 'application/json', 'accept': 'application/json'}
    # PUT to the layer URL.
    r = requests.put(url, auth=get_auth(), headers=headers, data=json.dumps(d))
    if not r.status_code == 200:
        r.raise_for_status()
    return r


def layer_getmap_extent(workspace, layer):
    """Utility function to download the layer full extent from the WMS endpoint, for monitoring purposes.
    """
    # First, obtain the bounding box from the layer resource URL.
    layer_resp = get_layer(workspace, layer)
    resource_href = layer_resp['layer']['resource']['href'].replace('http', 'https')
    resource = requests.get(resource_href, auth=get_auth())
    if not resource.status_code == 200:
        resource.raise_for_status()
    d = resource.json()
    bbox = d['featureType']['nativeBoundingBox']
    url = '{}/geoserver/{}/wms'.format(os.getenv('GEOSERVER_URL'), workspace)
    params = {
        'request': 'GetMap',
        'service': 'WMS',
        'version': '1.1.0',
        'layers': '{}:{}'.format(workspace, layer),
        'bbox': '{},{},{},{}'.format(bbox['minx'], bbox['miny'], bbox['maxx'], bbox['maxy']),
        'format': 'image/jpeg',
        'width': 256,
        'height': 256,
        'srs': d['featureType']['srs'],
    }
    r = requests.get(url, params=params)
    if not resource.status_code == 200:
        r.raise_for_status()
    return r


def query_wmts(save_tile=False):
    """Utility function to query WMTS layers and download tiles.
    """
    url = '{}/geoserver/gwc/service/wmts'.format(os.getenv('GEOSERVER_URL'))
    r = requests.get(url, params={'request': 'getcapabilities'})
    ns = {'wmts': 'http://www.opengis.net/wmts/1.0', 'ows': 'http://www.opengis.net/ows/1.1'}
    root = ET.fromstring(r.content)
    layers = root.findall('.//wmts:Layer', ns)
    for layer in layers:
        print(layer.find('ows:Identifier', ns).text.split(':')[1])
        params = {
            'layer': layer.find('ows:Identifier', ns).text,
            'style': '',
            'tilematrixset': layer.find('.//wmts:TileMatrixSet', ns).text,
            'Service': 'WMTS',
            'Request': 'GetTile',
            'Version': '1.0.0',
            'Format': 'image/jpeg',
            'TileMatrix': layer.find('.//wmts:TileMatrix', ns).text,
            'TileCol': layer.find('.//wmts:MaxTileCol', ns).text,
            'TileRow': layer.find('.//wmts:MaxTileRow', ns).text,
        }
        r = requests.get(url, params=params)
        if r.headers['Content-Type'] == 'image/jpeg':
            print('OK')
        else:
            print('ERROR')
        if save_tile:
            filename = '{}.jpg'.format(layer.find('ows:Identifier', ns).text.split(':')[1])
            with open('tiles/{}'.format(filename), 'wb') as f:
                r.raw.decode_content = True
                shutil.copyfileobj(r.raw, f)
