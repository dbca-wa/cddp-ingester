from bs4 import BeautifulSoup
import io
import json
import os
from osgeo import ogr
from qgis.core import QgsApplication, QgsVectorLayer
import requests
import tempfile
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
    if use_https and not resource_href.startswith('https'):
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


def convert_qml(gdb_path, layer, qml_path, logger=None):
    """Convert a QML style definition into an SLD. Returns the XML string.
    """
    # Ensure that the required Qt env var is set.
    if not os.getenv('QT_QPA_PLATFORM'):
        os.environ['QT_QPA_PLATFORM'] = 'offscreen'

    # Initialise a QGIS application.
    QgsApplication.setPrefixPath('/usr', True)
    qgis = QgsApplication([], False)
    qgis.initQgis()

    uri = '{}|layername={}'.format(gdb_path, layer)
    vector_layer = QgsVectorLayer(uri, layer, 'ogr')
    load_msg, load_success = vector_layer.loadNamedStyle(qml_path)
    if not load_success:
        if logger:
            logger.error('Error loading QML for {}: {}'.format(layer, load_msg))
        return

    sld_file = tempfile.NamedTemporaryFile(prefix=layer, suffix='.sld', delete=False)
    write_msg, write_success = vector_layer.saveSldStyle(sld_file.name)
    if not write_success:
        if logger:
            logger.error('Error writing SLD for {}: {}'.format(layer, write_msg))
        return

    # Define XML namespaces.
    ns = {
        'sld': 'http://www.opengis.net/sld',
        'se': 'http://www.opengis.net/se',
        'ogc': 'http://www.opengis.net/ogc',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
    }
    # Parse the SLD.
    root = ET.fromstring(sld_file.read())
    # Alter any Name elements where the element value == layer (uppercase).
    for el in root.findall('.//se:Name', ns):
        if el.text == layer:
            el.text = el.text.lower()
    # Alter any PropertyName element values to lowercase (these are db column names).
    for el in root.findall('.//ogc:PropertyName', ns):
        el.text = el.text.lower()
    # TODO: additional SLD cleansing.

    # Return the XML string.
    ET.register_namespace('', 'http://www.opengis.net/sld')  # Register default namespace
    for k, v in ns.items():  # Register remaining namespaces.
        ET.register_namespace(k, v)
    # Write a new XML tree to a file, then return the contents.
    tree = ET.ElementTree(root)
    f = io.StringIO()
    tree.write(f, encoding='unicode')
    f.seek(0)
    return f.read()
