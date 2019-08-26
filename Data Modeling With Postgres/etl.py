import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This function takes in a cursor variable and a filepath. 
    It reads in the filepath as a pandas dataframe, then sets 
    them equal to song_data, which is a subset of the song records,
    and then artist_data, which is a subset of the artist records.
    
    From there it uses the cursor to execute insert statements 
    from the sql_queries files to insert them into the DB 
    we have created.
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data =  df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This function takes in a cursor variable and a 
    filpath variable. Once again, this function also 
    reads in the filepath as a pandas dataframe.
    
    First we filter the dataframe by 'NextSong' and then 
    convert the 'ts' to a datetime column, as well as 
    pulling specific information from that timestamp. 
    
    we then zip the information into a dictionary and 
    iterate through the new dataframe we created called
    time_df to insert into our DB. 
    
    We use the sql_queries file to also insert user_df 
    and songplays_data into our Sparkify DB. 
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    filter_by_next = df['page'] == 'NextSong'
    df = df[filter_by_next]
    
    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    t = df['ts'].dt.strftime('%s,%H,%d,%U,%B,%Y,%A')
    
    # insert time data records
    time_data = sorted(list(t)) 
    
    times = []
    for x in time_data:
        times.append(x.split(','))
        
    column_labels = ['timestamp','hour', 'day_of_the_month', 'week_number_of_the_year', 'month', 'year', 'weekday']
    
    time_dict = {x:list(y) for x,y in zip(column_labels, zip(*times))}
    time_df = pd.DataFrame(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName', 'lastName', 'gender', 'level']]

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
        try:
            songplay_data =  (row['ts'], row['userId'], row['level'], songid, artistid, row['sessionId'], 
                             row['location'], row['userAgent'])
            cur.execute(songplay_table_insert, songplay_data)
        except Exception as e:
            print(e)


def process_data(cur, conn, filepath, func):
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    print('song_data done')
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    print('log_data done')

    conn.close()


if __name__ == "__main__":
    main()