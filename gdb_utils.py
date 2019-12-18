from bs4 import BeautifulSoup
import json
import os
from osgeo import ogr
import requests
import xml.etree.ElementTree as ET


def get_auth():
    return (os.getenv('GEOSERVER_USERNAME'), os.getenv('GEOSERVER_PASSWORD'))


def get_metadata(gdb_path, layer):
    """For a given file GDB path and layer, return the metadata XML string.
    """
    driver = ogr.GetDriverByName("OpenFileGDB")
    fgdb = driver.Open(gdb_path, 0)
    metadata_layer = fgdb.ExecuteSQL("GetLayerMetadata {}".format(layer))
    metadata_string = metadata_layer.GetFeature(0).GetFieldAsString(0)
    return metadata_string


def get_abstract(metadata):
    """For a given XML metadata string, return the abstract text (minus any markup).
    """
    root = ET.fromstring(metadata)
    abstract_element = root.find('./dataIdInfo/idAbs')
    if abstract_element is None:
        return None
    abstract_html = abstract_element.text
    abstract_text = BeautifulSoup(abstract_html, 'lxml').text.strip()
    return abstract_text


def get_title(metadata):
    """For a given XML metadata string, return the title.
    """
    root = ET.fromstring(metadata)
    title_element = root.find('./dataIdInfo/idCitation/resTitle')
    if title_element is None:
        return None
    return title_element.text


def get_resource(layer_href, use_https=True):
    """Get the resource object details for a layer. Returns a tuple of
    (resource_href, dict).
    """
    # First, get the layer's existing details.
    auth = get_auth()
    r = requests.get(layer_href, auth=auth)
    if not r.status_code == 200:
        r.raise_for_status()
    d = r.json()
    # Next retrieve the layer's resource URL and get those details.
    # We can infer this URL from the layer name, but let's be cautious.
    resource_href = d['layer']['resource']['href']
    if use_https:
        resource_href = resource_href.replace('http', 'https')
    r = requests.get(resource_href, auth=auth)
    if not r.status_code == 200:
        r.raise_for_status()
    return (resource_href, r.json())


def update_resource(layer_href, attr):
    """Update a layer's resource object using a passed-in dict on the resource attributes and values.
    """
    # Get the resource object.
    resource_href, d = get_resource(layer_href)
    for key, value in attr.items():
        d['featureType'][key] = value
    data = json.dumps(d)
    headers = {'content-type': 'application/json'}
    r = requests.put(resource_href, auth=get_auth(), headers=headers, data=data)
    if not r.status_code == 200:
        r.raise_for_status()
    return
