from multiprocessing import Pool
import os

from gdb_utils import get_metadata, get_abstract, get_title, update_resource, convert_qml
from utils import logger_setup, parse_cddp_qmls, get_layers, create_style, set_layer_style


# Configure logging.
LOGGER = logger_setup()


def update_metadata(dataset, layers):
    """Utility script to update the metadata for all the published layers in a given file GDB.
    This script also publishes styles for each layer, on the assumption that a compatible QML
    file named <layer>.qml is present.
    """
    gdb_path, layer, qml_path = dataset
    layer_name = layer.lower()
    workspace = os.getenv('GEOSERVER_WORKSPACE')
    # For a given dataset, find out if it is published. If so, parse the metadata from the fGDB.
    if layer_name in layers:
        # Metadata
        metadata = get_metadata(gdb_path, layer)
        if metadata:
            # Get the layer's REST endpoint.
            layer_href = layers[layer_name]
            # Update the published layer's metadata.
            abstract = get_abstract(metadata)
            if abstract:
                try:
                    update_resource(layer_href, {'abstract': abstract})
                    LOGGER.info('Updated abstract: {}'.format(layer_name))
                except:
                    LOGGER.exception('Error during update of abstract for {}'.format(layer_name))
            else:
                LOGGER.warning('No abstract available for {}'.format(layer_name))
            # Update the layer title from metadata.
            title = get_title(metadata)
            if title:
                try:
                    update_resource(layer_href, {'title': title})
                    LOGGER.info('Updated title: {}'.format(layer_name))
                except:
                    LOGGER.exception('Error during update of title for {}'.format(layer_name))
            else:
                LOGGER.warning('No title available for {}'.format(layer_name))
        else:
            LOGGER.warning('No metadata available for {}'.format(layer_name))

        # Styles
        sld_string = convert_qml(gdb_path, layer_name, qml_path, LOGGER)
        r = create_style(workspace, layer_name, sld_string)
        if r.status_code == 200:
            LOGGER.info('Style created: {}'.format(layer_name))
        elif r.status_code == 201:
            LOGGER.info('Style updated: {}'.format(layer_name))
        else:
            LOGGER.warning('Style not changed: {}'.format(layer_name))
        # Set the layer's default style.
        if r.status_code in [200, 201]:
            r = set_layer_style(workspace, layer_name)
            LOGGER.info('Layer default style updated: {}'.format(layer_name))


def mp_handler(cddp_path=None):
    """Multiprocessing handler to import metadata from file GDBs in the mounted CDDP volume.
    """
    if not cddp_path:
        # Assume that this path set via an environment variable if not explicitly passed in.
        cddp_path = os.getenv('CDDP_PATH')

    datasets = parse_cddp_qmls(cddp_path, LOGGER)
    workspace = os.getenv('GEOSERVER_WORKSPACE')
    layers = get_layers(workspace)
    LOGGER.info('{} datasets scheduled for metadata & style updates'.format(len(datasets)))

    # Use a multiprocessing Pool to update layer metadata in parallel.
    p = Pool(processes=4)
    iterable = [(dataset, layers) for dataset in datasets]
    p.starmap(update_metadata, iterable)


if __name__ == "__main__":
    mp_handler()
