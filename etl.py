import os
import glob
import psycopg2
import pandas as pd
from datetime import datetime
from sqlalchemy_schemadisplay import create_schema_graph
from sqlalchemy import MetaData
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Reads in a JSON song metadata file for a single song and inserts the song and artist data to song and artist tables in the db, respectively.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id','title','artist_id','year','duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Reads in a JSON user activity log file and inserts the relevant data into the time, user, and songplay tables in the db.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = df['ts'].apply(lambda ts: datetime.fromtimestamp(ts/1000))
    
    # insert time data records
    time_data = {'timestamp': df['ts'], 'hour': t.dt.hour, 'day': t.dt.day, 'week_of_year': t.dt.week, 'month': t.dt.month, 'year': t.dt.year, 'weekday': t.dt.weekday}
#     column_labels =  # created time_data as a dict originally, so no need for separately listing out column_labels here
    time_df = pd.DataFrame.from_dict(time_data)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - For all json files in the filepath, apply the given func (which should be process_song_file() or process_log_file()).
    - This runs the given func to ingest song or log json files and insert data into relevant db tables.
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

def create_er_diagram():
    """
    - Creates an entity relationship (ER) diagram in a png file named 'sparkifydb_erd.png' to visually display the tables
    - of the database to enable easy visual understanding of the db. Saves the png file in the folder from which etl.py 
    - (this script) is run.
    """
    er_diagram_file_name = 'sparkifydb_erd.png'
    graph = create_schema_graph(metadata=MetaData('postgresql://student:student@127.0.0.1/sparkifydb'))
    graph.write_png(er_diagram_file_name)
    print(f'ER diagram created as {er_diagram_file_name}')

def main():
    """
    - Connects to the sparkifydb and runs process_data() for both song and log data to insert data into relevant sparkifydb tables.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    create_er_diagram()

    conn.close()


if __name__ == "__main__":
    main()