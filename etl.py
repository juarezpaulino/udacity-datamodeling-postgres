import os
import glob
import psycopg2
import pandas as pd
import io
import csv

from sql_queries import *


def process_song_file(cur, filepath):
    """ ETL routine to load song data to postgres database

    Keyword arguments:
    cur -- cursor from psycopg connection
    filepath -- filepath to extract song data from JSON store
    """
    # open song file
    df = pd.read_json(filepath, encoding='utf8', lines=True)

    # insert song record
    song_data = list(df[[ 'song_id', 'title', 'artist_id', 'year', 'duration' ]].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[[ 'artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude' ]].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """ ETL routine to load log data to postgres database

    Keyword arguments:
    cur -- cursor from psycopg connection
    filepath -- filepath to extract log data from JSON store
    """
    # Some static variable to count songplay_id through all files.
    if "songplay_counter" not in process_log_file.__dict__: process_log_file.songplay_counter = 0
    
    # open log file
    df = pd.read_json(filepath, encoding='utf8', lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t.values, t.dt.hour.values, t.dt.day.values, t.dt.weekofyear.values, t.dt.month.values, t.dt.year.values, t.dt.weekday.values)
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    column_labels = ['user_id', 'first_name', 'last_name', 'gender', 'level']
    user_df = df.rename(
        columns={'userId':'user_id','firstName':'first_name','lastName':'last_name'}
    )[column_labels]

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    column_labels = ['songplay_id', 'start_time', 'user_id', 'song_id', 'artist_id', 'level', 'session_id', 'location', 'user_agent']
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        # Modified to do bulk insert through COPY command
        songplay_data = (
            process_log_file.songplay_counter,
            t[index],
            int(row.userId),
            songid,
            artistid,
            row.level,
            row.sessionId,
            row.location,
            row.userAgent
        )
        writer.writerow(songplay_data)
        process_log_file.songplay_counter += 1
        #cur.execute(songplay_table_insert, songplay_data)
     
    # Bulk insertion to songplays table
    buffer.seek(0)
    cur.copy_expert("""COPY songplays FROM STDIN WITH (FORMAT CSV)""", buffer)


def process_data(cur, conn, filepath, func):
    """ Traverses storage and process JSON files

    Keyword arguments:
    cur -- cursor from psycopg connection
    conn -- connection to postgres database held by the psycopg driver
    filepath -- base filepath directory where JSON files should be processed
    func -- ETL routine to process JSON files
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
    """
    - Fetch connection and cursor to postgres local database.

    - Process songs and logs JSON stores applying ETL routines.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()    


if __name__ == "__main__":
    main()