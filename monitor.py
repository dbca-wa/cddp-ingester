import os
import requests
import time
from utils import logger_setup, get_layers, layer_getmap_extent
import xml.etree.ElementTree as ET


# Configure logging.
LOGGER = logger_setup()


def monitor_layers_wms(workspace=None):
    """Utility script to download the largest extent for all published WMS layers.
    """
    if not workspace:
        workspace = os.getenv('GEOSERVER_WORKSPACE')
    LOGGER.info('Querying for published layers')
    layers = get_layers(workspace)
    success = 0
    failures = []

    # Query each WMS extent:
    for layer in layers.keys():
        r = layer_getmap_extent(workspace, layer)
        if r.headers['Content-Type'] == 'image/jpeg':
            LOGGER.info('Queried {}'.format(layer))
            success += 1
        else:
            LOGGER.warning('Failed to query {}'.format(layer))
            failures.append(layer)

        time.sleep(2)  # Pause 2 seconds between requests.

    LOGGER.info('{}/{} published layers successfully queried'.format(success, len(layers)))
    if failures:
        LOGGER.info('Failed layers: {}'.format(', '.join(failures)))


def monitor_layers(workspace=None):
    """Utility script to query each of the tile layers in a workspace, specifically one
    single tile of the most zoomed-in extent, in order to test the published style for
    that layer. This is faster than querying the whole extent of the dataset.

    XML tree to layer tile matrixes of the capability document:
    Contents
    - Layer (per published layer)
      - Title
      - TileMatrixSetLink (per-projection)
        - TileMatrixSet
        - TileMatrixSetLimits (per zoom level)
          - TileMatrixLimits
            - TileMatrix
            - MaxTileRow
            - MaxTileCol
    """
    if not workspace:
        workspace = os.getenv('GEOSERVER_WORKSPACE')
    success = 0
    failures = []
    LOGGER.info('Querying WMTS GetCapabilities document')
    url = '{}/geoserver/gwc/service/wmts'.format(os.getenv('GEOSERVER_URL'))
    r = requests.get(url, params={'request': 'getcapabilities'})
    ns = {'wmts': 'http://www.opengis.net/wmts/1.0', 'ows': 'http://www.opengis.net/ows/1.1'}
    root = ET.fromstring(r.content)
    layers = root.findall('.//wmts:Layer', ns)
    LOGGER.info('{} published layers queued to query'.format(len(layers)))
    for layer in layers:
        tmsl = layer.find('.//wmts:TileMatrixSetLink', ns)
        tml = tmsl.findall('.//wmts:TileMatrixLimits', ns)[-1]  # The last TileMatrixLimit is the most zoomed-in.
        params = {
            'layer': layer.find('ows:Identifier', ns).text,
            'style': '',
            'tilematrixset': tmsl.find('.//wmts:TileMatrixSet', ns).text,
            'Service': 'WMTS',
            'Request': 'GetTile',
            'Version': '1.0.0',
            'Format': 'image/jpeg',
            'TileMatrix': tml.find('.//wmts:TileMatrix', ns).text,
            'TileRow': tml.find('.//wmts:MaxTileRow', ns).text,
            'TileCol': tml.find('.//wmts:MaxTileCol', ns).text,
        }
        r = requests.get(url, params=params)
        layer_name = layer.find('ows:Identifier', ns).text.split(':')[1]
        if r.headers['Content-Type'] == 'image/jpeg':
            LOGGER.info('Queried {}'.format(layer_name))
            success += 1
        else:
            LOGGER.warning('Failed to query {}'.format(layer_name))
            failures.append(layer_name)

    LOGGER.info('{}/{} published layers successfully queried'.format(success, len(layers)))
    if failures:
        LOGGER.info('Failed layers: {}'.format(', '.join(failures)))

if __name__ == "__main__":
    monitor_layers()
