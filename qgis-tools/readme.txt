install_qgis

    Bash script to add UbuntiGIS's stable PPA and install qgis. Should work on recent debian based systems.  This is required before using the following tools.


python3 create_sdl.py fgdb_path1 [fgdb_path2 ...]

   Python script to convert qml symbology files to sdl.  It will iterate through the specified fgdbs and for each layer attempt to load a .qml file of similar name from that fgdb's parent directory.  For each layer that has an appropriate .qml file it will write a LAYER_NAME.sdl file to the fgdb's parent directory.


python3 extract_metadata.py fgdb_path1 [fgdb_path2 ...]

   Python script to extract layer metadata from fgdb.  It will iterate through the specified fgdbs and for each layer attempt to query it's metadata xml document and that document's abstract field.  For each layer that has appropriate metadata it will write LAYER_NAME.xml and LAYER_NAME.abstract.txt files to the fgdb's parent directory.
