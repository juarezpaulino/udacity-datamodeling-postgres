# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = " DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT NOT NULL
        , start_time TIMESTAMP
        , user_id INT
        , song_id TEXT
        , artist_id TEXT
        , level TEXT
        , session_id INT
        , location TEXT
        , user_agent TEXT
        , PRIMARY KEY (songplay_id)
        , FOREIGN KEY (start_time)
            REFERENCES time(start_time)
        , FOREIGN KEY (user_id)
            REFERENCES users(user_id)
        , FOREIGN KEY (song_id)
            REFERENCES songs(song_id)
        , FOREIGN KEY (artist_id)
            REFERENCES artists(artist_id)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT NOT NULL
        , first_name TEXT
        , last_name TEXT
        , gender CHAR
        , level TEXT
        , PRIMARY KEY (user_id)
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id TEXT NOT NULL
        , title TEXT
        , artist_id TEXT
        , year INT
        , duration FLOAT
        , PRIMARY KEY (song_id)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id TEXT NOT NULL
        , name TEXT
        , location TEXT
        , latitude FLOAT
        , longitude FLOAT
        , PRIMARY KEY (artist_id)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP NOT NULL
        , hour INT
        , day INT
        , week INT
        , month INT
        , year INT
        , weekday INT
        , PRIMARY KEY (start_time)
    );
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT(user_id) DO UPDATE SET level = EXCLUDED.level;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")


time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")

# FIND SONGS

song_select = ("""
    SELECT
        s.song_id
        , a.artist_id
    FROM songs s
    JOIN artists a
        ON s.artist_id = s.artist_id
    WHERE
        s.title = %s
        AND a.name = %s
        AND s.duration = %s;
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]