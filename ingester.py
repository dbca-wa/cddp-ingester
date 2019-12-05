from dotenv import load_dotenv
import logging
import os
import subprocess
import sys


# Configure logging.
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s | %(message)s')
handler.setFormatter(formatter)
LOGGER.addHandler(handler)


def ingest_cddp(cddp_path=None):
    '''This function expects the CDDP filepath to be passed in
    (e.g. /mnt/GIS-CALM/GIS1-Corporate/Data/GDB), in order to walk the path and locate
    file geodatabases for copying to the database.
    '''
    # Development environment: define variables in .env
    dot_env = os.path.join(os.getcwd(), '.env')
    if os.path.exists(dot_env):
        load_dotenv()

    if not cddp_path:
        # Assume that this path set via an environment variable if not explicitly passed in.
        cddp_path = os.getenv('CDDP_PATH')
    pg_string = 'host={} user={} password={} dbname={}'.format(
        os.getenv('DATABASE_HOST'),
        os.getenv('DATABASE_USERNAME'),
        os.getenv('DATABASE_PASSWORD'),
        os.getenv('DATABASE_NAME'),
    )
    ogr2ogr_cmd = 'ogr2ogr -overwrite -f PostgreSQL PG:"{pg_string}" {file_gdb} {layer_name}'

    gdb_paths = []
    for i in os.walk(cddp_path):
        if '/old/' in i[0]:  # Skip the 'old' subdirectories.
            continue
        if i[0].endswith('.gdb'):
            gdb_paths.append(i[0])

    for file_gdb in gdb_paths:
        LOGGER.info(file_gdb)

        try:
            gdb_layers = subprocess.check_output('ogrinfo -ro -so -q {}'.format(file_gdb), shell=True)
        except subprocess.CalledProcessError as e:
            LOGGER.exception('ERROR: ogrinfo step failed for {}'.format(file_gdb))
            LOGGER.info(e.cmd)
            continue

        layers = gdb_layers.splitlines()

        for layer in layers:
            layer_name = layer.split()[1].decode()
            LOGGER.info(layer_name)

            try:
                cmd = ogr2ogr_cmd.format(pg_string=pg_string, file_gdb=file_gdb, layer_name=layer_name)
                result = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as e:
                LOGGER.exception('ERROR: ogr2ogr step failed for layer {} in {}'.format(layer, file_gdb))
                LOGGER.info(e.cmd)
                continue

            # NONSTANDARD GEOMETRY TYPE HANDLING
            # The ogr2ogr copy operation might fail due to the dataset geometry not matching one of
            # the standard ones that the command expects (e.g. Multi Surface), but the command still
            # returns 0.
            # Check the stdout content for indicators that the copy failed.
            if b'COPY statement failed' in result and b'type Multi Surface' in result:
                LOGGER.warning('Copy statement failed, geometry type Multi Surface, trying explicit geom type')
                # Manually set the geometry type to MULTIPOLYGON:
                cmd = 'ogr2ogr -overwrite -f -nlt MULTIPOLYGON PostgreSQL PG:"{}" {} {}'.format(
                    pg_string, file_gdb, layer_name)
                try:
                    subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
                except subprocess.CalledProcessError as e:
                    LOGGER.exception('ERROR: ogr2ogr step failed for layer {} in {}'.format(layer, file_gdb))
                    LOGGER.info(e.cmd)
            elif b'COPY statement failed' in result and b'type Multi Curve' in result:
                LOGGER.warning('Copy statement failed, geometry type Multi Curve, trying explicit geom type')
                # Manually set the geometry type to MULTILINESTRING:
                cmd = 'ogr2ogr -overwrite -f -nlt MULTILINESTRING PostgreSQL PG:"{}" {} {}'.format(
                    pg_string, file_gdb, layer_name)
                try:
                    subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
                except subprocess.CalledProcessError as e:
                    LOGGER.exception('ERROR: ogr2ogr step failed for layer {} in {}'.format(layer, file_gdb))
                    LOGGER.info(e.cmd)


if __name__ == "__main__":
    ingest_cddp()
