#TODO: dask
#TODO: use rolling sum and a limit to control how much map data you download.  The full dataset is like 20TB, Too much man

#constants
TNM_URL = 'https://tnmaccess.nationalmap.gov/api/v1/products'
PAGE_SIZE = 1000
MAP_SAMPLE_RANDOM_STATE = 4918


#imports
import configparser
from pathlib import Path
import json
import pandas as pd
from tqdm import tqdm
import requests

tqdm.pandas()

def main():
    #load config
    config = configparser.ConfigParser()
    config.read('../config.ini')

    #Set up map directory

    map_directory_path = config['DEFAULT']['MapFolder']

    if map_directory_path:
        map_directory_path = Path(map_directory_path)
    else:
        #in the case of an empty string, which we use for default behavior, use Path.home()/elevation_maps
        map_directory_path = Path.home().joinpath("./elevation_maps")

        #unsure whether or not I want to do this.  Note it does require stripping comments from config file.

        # config['DEFAULT']['MapFolder'] = map_directory_path.absolute().as_posix()
        # with open('../config.ini','w') as configfile:
        #     config.write(configfile)

    print(f"Using {map_directory_path} as map directory path")

    if not map_directory_path.parent.exists():
        raise FileNotFoundError(f"Could not create {map_directory_path}, as its parent does not exist.")

    if not map_directory_path.exists():
        print(f"Creating folder {map_directory_path}")
        map_directory_path.mkdir()




    #get download info
    print("Fetching map download data")

    #todo: custom handle the possible failures here
    req = requests.get(TNM_URL,params={'datasets':config['DEFAULT']['DataSet'],'prodFormats':'GeoTIFF'})

    cnt = json.loads(req.content)

    print(f"Received {cnt['total']}, {cnt['filteredOut']} filtered out")

    #cprint red
    if cnt['errors']:
        print(f"Errors: {cnt['errors']}")
    else:
        print("No errors")

    #maybe remove this or the above?
    print("Messages:")
    for m in cnt['messages']:
        print('\t> ',m)

    #start the download prep
    download_df = pd.json_normalize(cnt['items'],sep="_")

    #get the rest of the data rows
    #TODO: consider checking if download_df.csv already exists, maybe compare its size to cnt['total'] - filtered
    print("assembling download df")
    for n in tqdm(range(PAGE_SIZE, cnt['total']+PAGE_SIZE,PAGE_SIZE)):
        new_req = requests.get(TNM_URL,params={'datasets':config['DEFAULT']['DataSet'],'max':1000,'offset':n})
        new_content = json.loads(new_req.content)
        for m in new_content['messages']:
            print(f'[{n}]:  {m}')
        download_df = pd.concat([download_df,pd.json_normalize(new_content['items'],sep='_')])

    #Dedupe
    size_before_dedupe = download_df.shape[0]
    #YYYY-MM-DD format, so we can just text sort
    download_df = download_df.sort_values(by=['lastUpdated','dateCreated'],ascending=False)
    download_df = download_df.drop_duplicates(subset=['boundingBox_minX', 'boundingBox_maxX','boundingBox_minY',
                                                  'boundingBox_maxY'],keep='first')

    print(f"Removed {size_before_dedupe-download_df.shape[0]} bounding box duplicates")

    if (maxMaps:=int(config['DEFAULT']["MaxMaps"])):
        download_df=download_df.sample(n=maxMaps,random_state=MAP_SAMPLE_RANDOM_STATE)
        print(f'downsampled to {download_df.shape[0]} maps')

    ##DEBUG: REMOVE
    # download_df = download_df.iloc[:5]

    #Assigns download paths
    def make_download_path(source_id,directory=map_directory_path):
        directory = Path(directory)
        path = directory.joinpath('.',source_id).with_suffix('.tif')
        return path.absolute().as_posix()

    #downloads the data
    def download_data(row):
        download_path = make_download_path(row.sourceId)
        if not (pdp:=Path(download_path)).exists():
            print(f'Downloading to {download_path}')
            content = requests.get(row.downloadURL).content
            pdp.write_bytes(content)
        return download_path

    download_df['download_path'] = download_df.progress_apply(download_data,axis=1)

    #overwrites every time.

    download_df_path = Path(config['DEFAULT']['DownloadDF']).absolute().as_posix()
    download_df.to_csv(download_df_path)

if __name__=='__main__':
    main()
