import os
import glob
import time
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Processes given JSON file (filepath) and populates the song and artist tables.
    
    Args:
        cur (Object): Postgres Cursor (psycopg2.conn.cur())
        filepath (string): relative location of the JSON file containing song data
            
    Returns:
        None
    
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = [list(row) for row in df[['song_id','title','artist_id','year','duration']].itertuples(index=False)]
    for song in song_data:
        cur.execute(song_table_insert, song)
    
    # insert artist record
    artist_data = [list(row) for row in df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].itertuples(index=False)]
    for artist in artist_data:
        cur.execute(artist_table_insert, artist)


def process_log_file(cur, filepath):
    """Processes given JSON file (filepath) and populates the songplay and user tables.
        
    Args:
        cur (Object): Postgres Cursor (psycopg2.conn.cur())
        filepath (string): relative location of the JSON file containing songplay log data
                
    Returns:
        None
    
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.query("page == 'NextSong'")

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'],unit='ms')
    
    # insert time data records
    time_data = (t,t.dt.hour,t.dt.day,t.dt.weekofyear,t.dt.month,t.dt.year,t.dt.dayofweek)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels,time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    startTime = time.perf_counter()
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
    print(f'Elapsed time: {time.perf_counter() - startTime} seconds for INSERT')


            
    # insert songplay records - COPY version
#     startTime = time.perf_counter()
#     songplays_df = pd.DataFrame(columns=['start_time',
#                             'user_id', 'level', 'song_id',
#                             'artist_id', 'session_id', 'location',
#                             'user_agent'])
#     songplays_df.index.name = 'songplay_id'
#     for index, row in df.iterrows():
        
    
#     # get songid and artistid from song and artist tables
#         cur.execute(song_select, (row.song, row.artist, row.length))
#         results = cur.fetchone()
        
#         if results:
#             songid, artistid = results
#         else:
#             songid, artistid = None, None

#         # add songplay record to dataframe
#         songplays_df.loc[len(songplays_df)] = [pd.to_datetime(row.ts), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        
#     # insert songplay log records using copy (really should use SQLAlechemy for this one as psycopg2 requires this intermediate file hack.)
#     temp_file = 'songplays_temp.csv'
#     table = 'songplays'
#     songplays_df.to_csv(temp_file,index=False)
#     cur.copy_expert(sql=songplay_table_copy % table, file=open(temp_file, 'r'))
#     os.remove(temp_file)
#     print(f'Elapsed time: {time.perf_counter() - startTime} seconds for COPY')

    
def process_data(cur, conn, filepath, func):
    """Walks through the given directories (filepath) and passes the resulting list of files through the song and songplay processing pipeline.
    
    Args:
        cur (Object): Postgres Cursor (psycopg2.conn.cur())
        conn (Object): Postgres Cursor (psycopg2.conn
        filepath (str): directory containing log or song JSON files
        func (function): data processing function, either process_song_file or process_log_file
        
    Returns:
        None
    
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """Runs the ETL pipeline for the sparkifydb project
    
    Args:
        None
        
    Returns:
        None
    
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()