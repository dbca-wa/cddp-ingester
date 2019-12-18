from dotenv import load_dotenv
from multiprocessing import Pool, Value
import os
import subprocess

from utils import logger_setup, parse_cddp, get_available_layers, publish_layer


# Configure logging.
LOGGER = logger_setup()
# Init a counter variable.
COUNTER = Value('i', 0)


# Development environment: define variables in .env
dot_env = os.path.join(os.getcwd(), '.env')
if os.path.exists(dot_env):
    load_dotenv()


def ingest_layer(data):
    """This function expects to be passed a tuple containing (path, layer_name) pairs for
    import to a PostgreSQL database using ogr2ogr.
    """
    file_gdb, layer_name = data[0], data[1]
    pg_string = 'host={} user={} password={} dbname={}'.format(
        os.getenv('DATABASE_HOST'),
        os.getenv('DATABASE_USERNAME'),
        os.getenv('DATABASE_PASSWORD'),
        os.getenv('DATABASE_NAME'),
    )
    ogr2ogr_cmd = 'ogr2ogr -overwrite -f PostgreSQL PG:"{pg_string}" {file_gdb} {layer_name}'
    LOGGER.info('Copying layer {}'.format(layer_name))

    try:
        cmd = ogr2ogr_cmd.format(pg_string=pg_string, file_gdb=file_gdb, layer_name=layer_name)
        result = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError:
        LOGGER.exception('ogr2ogr step failed for layer {} in {}'.format(layer_name, file_gdb))
        return

    # NONSTANDARD GEOMETRY TYPE HANDLING
    # The ogr2ogr copy operation might fail due to the dataset geometry not matching one of
    # the standard ones that the command expects (e.g. Multi Surface), but the command still
    # returns 0.
    # Check the stdout content for indicators that the copy failed.
    if b'COPY statement failed' in result and b'type Multi Surface' in result:
        LOGGER.warning('Copy statement failed, geometry type Multi Surface, trying explicit geom type')
        # Manually set the geometry type to MULTIPOLYGON:
        cmd = 'ogr2ogr -overwrite -nlt MULTIPOLYGON -f PostgreSQL PG:"{}" {} {}'.format(
            pg_string, file_gdb, layer_name)
        try:
            subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError:
            LOGGER.exception('ogr2ogr step failed for layer {} in {}'.format(layer_name, file_gdb))
            return
    elif b'COPY statement failed' in result and b'type Multi Curve' in result:
        LOGGER.warning('Copy statement failed, geometry type Multi Curve, trying explicit geom type')
        # Manually set the geometry type to MULTILINESTRING:
        cmd = 'ogr2ogr -overwrite -nlt MULTILINESTRING -f PostgreSQL PG:"{}" {} {}'.format(
            pg_string, file_gdb, layer_name)
        try:
            subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError:
            LOGGER.exception('ogr2ogr step failed for layer {} in {}'.format(layer_name, file_gdb))
            return

    global COUNTER  # Couldn't work out how to do this without using a global var :|
    with COUNTER.get_lock():
        COUNTER.value += 1
    LOGGER.info('Layer {} completed'.format(layer_name))


def mp_handler(cddp_path=None):
    """Multiprocessing handler to import file GDBs from the mounted CDDP volume.
    """
    if not cddp_path:
        # Assume that this path set via an environment variable if not explicitly passed in.
        cddp_path = os.getenv('CDDP_PATH')

    datasets = parse_cddp(cddp_path, LOGGER)
    LOGGER.info('{} layers scheduled for copying from file GDB'.format(len(datasets)))

    # Use a multiprocessing Pool to ingest datasets in parallel.
    p = Pool(processes=4)
    p.map(ingest_layer, datasets)
    LOGGER.info('{}/{} layers successfully copied'.format(COUNTER.value, len(datasets)))


def publish_layers():
    """Function to check if any new layers are present and can be published.
    """
    workspace = os.getenv('GEOSERVER_WORKSPACE')
    datastore = os.getenv('GEOSERVER_DATASTORE')
    blacklist = ['pg_buffercache', 'pg_stat_statements']  # TODO: don't hardcode this.
    LOGGER.info('Checking for any new layers to publish')
    layers = get_available_layers(workspace, datastore)

    for layer in layers:
        if layer not in blacklist:
            try:
                publish_layer(workspace, datastore, layer)
                LOGGER.info('Published layer {}'.format(layer))
            except:
                LOGGER.exception('Publish layer failed for {}'.format(layer))
                continue


if __name__ == "__main__":
    mp_handler()
    publish_layers()
