from pathlib import Path as _Path
from pyproj.crs import CRS as _CRS
import numpy as _np
from scipy.ndimage import convolve as _convolve

# going back and forth on get_geod looking up from a dict.  I suppose instantiate every time
# so that you can more easily dask things. Instantiating one doesn't require much.


#return proj4 string, not geod, which we'll look up from a dict.

def get_crs_string(dataset):
    '''
    dataset should be an open rasterio file object
    :param dataset:
    :return: CRS string, e.g. EPSG:26916
    '''
    crs = dataset.crs
    crs_string = crs.to_string()
    return crs_string

## Not sure if this will work if I want to dask it
def get_geod(crs_string):
    '''
    Take in a CRS string.
    :param crs_string: EPSG:26916
    :return: pyproj.geod.Geod
    '''
    new_geod =_CRS.from_string(crs_string)
    return new_geod

#Used chatGPT to help here, modified to return a single score
def calculate_tri(elevation_array):
    # Define the convolution kernel to calculate the sum of the differences
    kernel = _np.array([[1, 1, 1],
                       [1, -8, 1],
                       [1, 1, 1]])

    # Apply convolution to get the sum of differences
    diff_sum = _convolve(elevation_array, kernel, mode='constant', cval=0.0)

    # Calculate the number of neighbors
    num_neighbors = _convolve(_np.ones_like(elevation_array), _np.abs(kernel), mode='constant', cval=0.0)

    # Compute the mean absolute differences (TRI)
    tri_array = _np.abs(diff_sum) / num_neighbors

    #reduce to a single score
    tri = tri_array.mean()
    return tri

