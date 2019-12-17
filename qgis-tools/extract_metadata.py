from os import path
from sys import argv
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup

from osgeo import ogr
from qgis.core import QgsApplication, QgsVectorLayer


def extract_metadata_for_fgdb_layer(fgdb, fgdb_path, layer_name):
    parent_dir = path.abspath(path.join(fgdb_path, '..'))

    metadata_layer = fgdb.ExecuteSQL("GetLayerMetadata {}".format(layer_name))
    metadata_string = metadata_layer.GetFeature(0).GetFieldAsString(0)
    if not metadata_string:
        print("No metadata found for {} in {}".format(layer_name, fgdb_path))
        return

    metadata_path = path.join(parent_dir, "{}.xml".format(layer_name))
    with open(metadata_path, "w+") as metadata_file:
        metadata_file.write(metadata_string)

    root = ET.fromstring(metadata_string)
    abstract_element = root.find("./dataIdInfo/idAbs")

    if abstract_element is None:
        print("No abstact element for {} in {}".format(layer_name, fgdb_path))
        return

    abstract_html = abstract_element.text
    abstract_text = BeautifulSoup(abstract_html, "lxml").text.strip()

    if not abstract_text:
        print("No abstact text for {} in {}".format(layer_name, fgdb_path))
        return

    abstract_path = path.join(parent_dir, "{}.abstract.txt".format(layer_name))
    with open(abstract_path, "w+") as abstract_file:
        abstract_file.write(metadata_string)

    print("Complete: {}".format(layer_name))


def extract_metadata_for_fgdb_layers(fgdb_path):
    if not path.isdir(fgdb_path):
        print("Error: FGDB not found at {}".format(fgdb_path))
        return

    driver = ogr.GetDriverByName("OpenFileGDB")
    fgdb = driver.Open(fgdb_path, 0)
    layer_names = [l.GetName() for l in fgdb]

    for layer_name in layer_names:
        extract_metadata_for_fgdb_layer(fgdb, fgdb_path, layer_name)


def main():
    QgsApplication.setPrefixPath("/usr", True)
    qgis = QgsApplication([], False)
    qgis.initQgis()

    params = argv[1:]
    if len(params) > 0:
        for param in params:
            extract_metadata_for_fgdb_layers(param)
    else:
        print("Usage: extract_metadata.py fgdb_path [fgdb_path2 ...]")

    qgis.exitQgis()


if __name__ == "__main__":
    main()