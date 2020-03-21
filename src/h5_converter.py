import os
import sys
import glob
import pandas as pd

'''
Currently the code only extracts 'songs' tables from the H5 files.
Such as artist_terms, bars_confidence, sections_start, etc (numpy array data) are missing.
List of fields and shape: http://millionsongdataset.com/pages/example-track-description/
'''

def progress(count, total, suffix=''):
    bar_len = 50
    filled_len = int(round(bar_len * count / float(total)))
    percents = round(100.0 * count / float(total), 1)
    bar = '#' * filled_len + '-' * (bar_len - filled_len)
    sys.stdout.write('[%s] %s%s %s\r' % (bar, percents, '%', suffix))
    sys.stdout.flush()

def get_filepath(basedir, ext = 'h5'):
    '''Collecting all the H5 files path in the given directory'''
    all_filepath = []
    for root, dirs, files in os.walk(basedir):
        files = glob.glob(os.path.join(root, '*.' + ext) )
        filies_abspath = [ os.path.abspath(f) for f in files ]
        all_filepath.extend( filies_abspath )
    return all_filepath

def files_to_csv(all_filepath, csv_path):
    df = pd.DataFrame()
    total_file = len(all_filepath)
    for i, f in enumerate(all_filepath):
        store = pd.HDFStore(f)
    #Extracting tables
        analysis = pd.read_hdf(store,'/analysis/songs')
        metadata =  pd.read_hdf(store, '/metadata/songs')
        musicbrainz = pd.read_hdf(store, '/musicbrainz/songs')
        song = pd.concat( [analysis, metadata, musicbrainz], axis = 1)
        df = df.append(song)
        store.close()
        progress( i, total_file, 'of files extracted!')
    #Saving to csv
    print('Started writing csv!')
    df.to_csv( csv_path )
    print('Job done!')

def main():
    data_dir = 'data'
    basedir = os.path.join(data_dir, 'MillionSongSubset', 'data' )
    csv_name = 'MillionSong_full.csv'
    csv_path = os.path.join(data_dir, csv_name)
    print('Extracting H5 files from the folder: ' + basedir)
    all_filepath = get_filepath(basedir)
    files_to_csv(all_filepath, csv_path)

if __name__ == "__main__":  
    main()
    #pass
