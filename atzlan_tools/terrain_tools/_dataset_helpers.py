import rasterio as _rio
from pathlib import Path as _Path
import warnings as _warnings
import numpy as _np
import pyproj as _pyproj

#TODO: Rename file

print("Debug: loading module")

#intended to be especially used with dask


class RandomSection:
    def __init__(self, rio_dataset_helper, mode=None, random_state=None, name=None, pixel_cut_size=None):
        if mode == 'pixel':
            if pixel_cut_size is None:
                raise ValueError(pixel_cut_size)
            self.pixel_cut_size = int(pixel_cut_size)
        if mode != 'pixel':
            #TODO: add geo mode
            raise ValueError("unsupported mode requested")

        self.rdh = rio_dataset_helper
        self.mode = mode
        self.name = name
        self.random_state = random_state
        self.local_rng = _np.random.default_rng(random_state)

        #set up the cut dimensions
        h, w = self.rdh.arr.shape
        self.top = self.local_rng.integers(0, h - self.pixel_cut_size + 1)
        self.left = self.local_rng.integers(0, w - self.pixel_cut_size + 1)
        self.bottom = self.top + self.pixel_cut_size
        self.right = self.left + self.pixel_cut_size
        #delete these so I don't accidentally use them
        del h, w

        #get the actual array
        self.arr = self.rdh.arr[self.top:self.bottom, self.left:self.right]

        #get the lon,lat bounds
        lx, by = self.rdh.xy(self.bottom, self.left)
        rx, ty = self.rdh.xy(self.top, self.right)

        self.west, self.south = self.rdh.transformer.transform(lx, by)
        self.east, self.north = self.rdh.transformer.transform(rx, ty)

class RioDatasetHelper:
    def __init__(self, path, name=None):
        #if it's not a dataset, we'll try to treat it as a path
        path = _Path(path)
        self.path = path
        self.name = name
        with _rio.open(path, 'r') as ds:
            if ds.indexes != (1,):
                _warnings.warn(f"Index is expected to be (1,), but was instead {self.ds.indexes}")
            self.arr = ds.read(1)
            self.bounds = ds.bounds
            self.crs = ds.crs
            self.width = ds.width
            self.height = ds.height
            self.transform_crs = "EPSG:4326"
            self.xy = ds.xy
            self.transformer = _pyproj.Transformer.from_crs(self.crs, self.transform_crs, always_xy=True)

