from os import path
from sys import argv

from osgeo import ogr
from qgis.core import QgsApplication, QgsVectorLayer


#fgdb_path = r"C:\temp\fgdb\Administration__Boundaries\Administrative_Boundaries.gdb"
fgdb_path = r"/home/kelly/data/fgdb/Administration_Boundaries/Administrative_Boundaries.gdb"


def create_sdl_for_fgdb_layer(fgdb_path, layer_name):
    parent_dir = path.abspath(path.join(fgdb_path, '..'))
    qml_path = path.join(parent_dir, "{}.qml".format(layer_name))
    sld_path = path.join(parent_dir, "{}.sld".format(layer_name))

    if not path.isfile(qml_path):
        print("Missing QML:", layer_name)
        return

    uri = "{}|layername={}".format(fgdb_path, layer_name)
    layer = QgsVectorLayer(uri, layer_name, "ogr")

    load_msg, load_success = layer.loadNamedStyle(qml_path)
    if not load_success:
        print("Error loading QML: {}, {}".format(layer_name, load_msg))
        return

    write_success = layer.saveSldStyle(sld_path)
    if not write_success:
        print("Error writing SDL: {}".format(layer_name))
        return

    print("Complete: {}".format(layer_name))


def create_sdl_for_fgdb_layers(fgdb_path):
    if not path.isdir(fgdb_path):
        print("Error: FGDB not found at {}".format(fgdb_path))
        return

    driver = ogr.GetDriverByName("OpenFileGDB")
    gdb_data = driver.Open(fgdb_path, 0)
    layer_names = [l.GetName() for l in gdb_data]

    for layer_name in layer_names:
        create_sdl_for_fgdb_layer(fgdb_path, layer_name)


def main():
    QgsApplication.setPrefixPath("/usr", True)
    qgis = QgsApplication([], False)
    qgis.initQgis()

    params = argv[1:]
    if len(params) > 0:
        for param in params:
            create_sdl_for_fgdb_layers(param)
    else:
        print("Usage: create_sdl.py fgdb_path [fgdb_path2 ...]")

    qgis.exitQgis()


if __name__ == "__main__":
    main()