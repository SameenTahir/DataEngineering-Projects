import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import configparser
import csv

def main():
    config = configparser.ConfigParser()
    config.read('config.conf')
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    cur.execute("SELECT songs.song_id, songs.artist_id FROM songs JOIN artists ON songs.artist_id=artists.artist_id")
    print(cur.fetchall())
    print("non-empty song")
    cur.execute("SELECT song_id, artist_id FROM songplays WHERE song_id IS NOT NULL")
    print(cur.fetchall())
    print("non-empty artist")
    cur.execute("SELECT song_id, artist_id FROM songplays WHERE artist_id IS NOT NULL")
    print(cur.fetchall())
    print("non-empty song and artist")
    cur.execute("SELECT song_id, artist_id FROM songplays WHERE song_id IS NOT NULL and artist_id IS NOT NULL")
    print(cur.fetchall())
    conn.close()
if __name__ == "__main__":
    main()