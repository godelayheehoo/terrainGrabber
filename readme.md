# WIP: This readme is mostly notes to myself.

# Requirements
- rasterio
- pandas
- matplotlib
- seaborn
- dask
- h5py
- pyproj

# Sampling
We separate the base maps (.tif files) into train and test sets.  Within a set, we do allow subsectioned terrain to
overlap 

# Config File

## MapFolder
This is the root folder that will hold the maps, e.g. D:/elevation_maps/. If left blank, it'll be placed in 
`Path.home()/elevation_maps/`, and the first run of download_maps.py will update this.

## DataSet
Dataset to download from, see https://apps.nationalmap.gov/datasets/.  Only Digital Elevation Model (DEM) 1 meter has 
been tested.