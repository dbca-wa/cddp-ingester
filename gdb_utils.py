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


def update_abstract(layer_href, abstract):
    """Update the abstract string for a published layer.
    """
    # First, get the layer's existing details.
    auth = get_auth()
    r = requests.get(layer_href, auth=auth)
    if not r.status_code == 200:
        r.raise_for_status()
    d = r.json()
    # Next retrieve the layer's resource URL and get those details.
    # We can infer this URL from the layer name, but let's be cautious.
    resource_href = d['layer']['resource']['href'].replace('http', 'https')
    r = requests.get(resource_href, auth=auth)
    d = r.json()
    if not r.status_code == 200:
        r.raise_for_status()
    # Update the abstract, then PUT to the resource URL.
    d['featureType']['abstract'] = abstract
    data = json.dumps(d)
    headers = {'content-type': 'application/json'}
    r = requests.put(resource_href, auth=auth, headers=headers, data=data)
    if not r.status_code == 200:
        r.raise_for_status()
    return
