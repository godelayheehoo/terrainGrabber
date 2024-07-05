#TODO: have each sectioning fxn return a count of tiles in the file, then sum those and print the result

import configparser
from pathlib import Path

import dask.bag
import h5py
import numpy as np
import dask.distributed as dd
import sys

sys.path.append(Path(__file__).joinpath('../atzlan_tools/').absolute().as_posix())

from atzlan_tools import terrain_tools as tt

#Contants
MASTER_SEED = 654

#setup
master_rng = np.random.default_rng(MASTER_SEED)

config = configparser.ConfigParser()
config.read('../config.ini')

map_directory_path = config['DEFAULT']['MapFolder']
if map_directory_path:
    map_directory_path = Path(map_directory_path)
else:
    # in the case of an empty string, which we use for default behavior, use Path.home()/elevation_maps
    map_directory_path = Path.home().joinpath("./elevation_maps")

#Setup hdfs folder
h5_directory_path = config['DEFAULT']['H5Folder']
if h5_directory_path:
    h5_directory_path = Path(h5_directory_path)
else:
    h5_directory_path = Path.home().joinpath("./h5_data")

if not h5_directory_path.parent.exists():
    raise FileNotFoundError(f"Could not find h5 directory path parent folder, {h5_directory_path.parent}")
h5_directory_path.mkdir(exist_ok=True)


def main():
    #setup dask
    client = dd.Client()
    print(client)
    print(f'Dashboard link: {client.dashboard_link}')

    # get list of map
    map_file_list = list(Path(map_directory_path).iterdir())

    #associate seeds
    map_file_list = list(zip(map_file_list, master_rng.integers(1, 1000000, len(map_file_list))))

    #define writing function
    def make_sections(source_filepath, map_seed):
        source_filepath = Path(source_filepath)
        map_seed = int(map_seed)

        # determine file name
        source_name = source_filepath.with_suffix('').name
        h5_filepath = h5_directory_path.joinpath('.', source_name).with_suffix('.hdf5')

        #get the number of already present cuts
        if h5_filepath.exists():
            with h5py.File(h5_filepath, 'r') as h5file:
                number_existing_cuts = len(h5file)
        else:
            number_existing_cuts = 0

        #determine number of cuts needed
        number_sections = config['SystemParameters.H5Creation'].getint('TilesPerMap') - number_existing_cuts

        #make the seeds for the cut
        local_rng = np.random.default_rng(map_seed)
        seeds = local_rng.integers(1, 1000000, number_sections)

        #get the RDH
        rdh = tt.RioDatasetHelper(source_filepath, name=source_name)

        #get the cut size
        square_size = config['SystemParameters.DataProcessing'].getint('SquareSize')

        with h5py.File(h5_filepath, 'a') as h5file:
            h5file.attrs['source_path'] = h5_filepath.absolute().as_posix()
            for s in seeds:
                # print(f"Using seeds {s}")
                cut_name = f'{source_name}_{s}'

                #skip the processing if we already have this one
                if cut_name in h5file:
                    continue
                rs = tt.RandomSection(rdh, mode='pixel', random_state=s, name=cut_name, pixel_cut_size=square_size)
                #If we have bad data, skip
                if (rsarrmin:=rs.arr.min()) < config['SystemParameters.DataCleaning'].getfloat('LowestAllowableElevation'):
                    print(f'cut {cut_name} has min elev {rsarrmin}, skipping')
                    continue

                #store the elevation array
                h5file[cut_name] = rs.arr

                #store the attributes
                for k in ['pixel_cut_size', 'mode', 'name', 'random_state', 'top', 'left', 'bottom', 'right', 'west',
                          'south', 'east', 'north']:
                    h5file[cut_name].attrs[k] = rs.__getattribute__(k)

                tri = tt.calculate_tri(rs.arr)
                h5file[cut_name].attrs['terrain_irregularity'] = tri
    #actually map in the function
    processing_bag = dask.bag.from_sequence(map_file_list)
    for_compute = processing_bag.map(lambda x: make_sections(*x))

    print('starting compute')
    for_compute.compute()


if __name__ == '__main__':
    main()
