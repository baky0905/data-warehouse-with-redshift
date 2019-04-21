import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS factsongplay"
user_table_drop = "DROP TABLE IF EXISTS dimusers"
song_table_drop = "DROP TABLE IF EXISTS dimsongs"
artist_table_drop = "DROP TABLE IF EXISTS dimartists"
time_table_drop = "DROP TABLE IF EXISTS dimtime"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(artist TEXT, \
                       auth TEXT, \
                       firstName TEXT, \
                       gender TEXT, \
                       itemInSession BIGINT, \
                       lastName TEXT, \
                       length DECIMAL, \
                       level TEXT, \
                       location TEXT, \
                       method TEXT, \
                       page TEXT, \
                       registration DECIMAL, \
                       sessionId INT, \
                       song TEXT, \
                       status INT, \
                       ts BIGINT, \
                       userAgent TEXT, \
                       userId TEXT );""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(song_id TEXT, \
                       num_songs INT, \
                       title TEXT, \
                       artist_name TEXT, \
                       artist_latitude DECIMAL, \
                       year INT, \
                       duration DECIMAL, \
                       artist_id TEXT, \
                       artist_longitude DECIMAL, \
                       artist_location TEXT);""")


user_table_create = ("""CREATE TABLE IF NOT EXISTS dimusers (user_id TEXT PRIMARY KEY, \
                       first_name    TEXT, \
                       last_name     TEXT, \
                       gender        TEXT, \
                       level         TEXT);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS dimsongs (song_id TEXT PRIMARY KEY, \
                       title         TEXT, \
                       artist_id     TEXT NOT NULL, \
                       year          INT, \
                       duration      DECIMAL);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS dimartists (artist_id TEXT PRIMARY KEY, \
                       name          TEXT NOT NULL, \
                       location      TEXT, \
                       latitude      DECIMAL, \
                       longitude     DECIMAL);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS dimtime (start_time DATE PRIMARY KEY, \
                       hour          INT, \
                       day           INT, \
                       week          INT, \
                       month         INT, \
                       year          INT, \
                       weekday       INT);""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS factsongplay (songplay_id INT IDENTITY(0,1), \
                       start_time    DATE NOT NULL, \
                       user_id       TEXT NOT NULL, \
                       level         TEXT NOT NULL, \
                       song_id       TEXT NOT NULL, \
                       artist_id     TEXT NOT NULL, \
                       session_id    INT, \
                       location      TEXT, \
                       user_agent    TEXT);""")


# STAGING TABLES

staging_songs_copy = ("""COPY staging_songs \
FROM 's3://udacity-dend/song-data' \
credentials '' \
compupdate off region 'us-west-2' \
FORMAT AS JSON 'auto' TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL maxerror 50000; """)

staging_events_copy = ("""COPY staging_events \
FROM 's3://udacity-dend/log-data' \
credentials '' \
compupdate off region 'us-west-2' \
FORMAT AS JSON 's3://udacity-dend/log_json_path.json' TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL maxerror 50000; """)

# FINAL TABLES



user_table_insert = ("""INSERT INTO dimusers (user_id, first_name, last_name, gender, level) \
                        SELECT DISTINCT userId AS user_id, \
                                 firstName AS first_name, \
                                 lastName AS last_name, \
                                 gender AS gender, \
                                 level AS level \
                        FROM staging_events \
                        WHERE page='NextSong';""")

song_table_insert = ("""INSERT INTO dimsongs(song_id, title, artist_id, year, duration) \
                        SELECT DISTINCT song_id AS song_id, \
                               title AS title, \
                               artist_id AS artist_id, \
                               year AS year, \
                               duration AS duration \
                        FROM staging_songs \
                        WHERE artist_id IS NOT NULL;""")

artist_table_insert = ("""INSERT INTO dimartists (artist_id, name, location, latitude, longitude) \
                            SELECT DISTINCT artist_id AS artist_id, \
                            artist_name AS name, \
                            artist_location AS location, \
                            artist_latitude AS latitude, \
                            artist_longitude AS longitude \
                            FROM staging_songs \
                            WHERE artist_name IS NOT NULL;""")

time_table_insert = ("""INSERT INTO dimtime (start_time, hour, day, week, month, year, weekday) \
                        SELECT timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time, \
                                extract(hour from start_time) as hour, \
                                extract(day from start_time) as day, \
                                extract(week from start_time) as week, \
                                extract(month from start_time) as month, \
                                extract(year from start_time) as year, \
                                extract(weekday from start_time) as weekday \
                        FROM staging_events \
                        WHERE page='NextSong';""")

songplay_table_insert = ("""INSERT INTO factsongplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
                            SELECT timestamp 'epoch' + se.ts/1000 * interval '1 second' AS start_time, \
                                    se.userId AS user_id, \
                                    se.level AS level, \
                                    ss.song_id AS song_id, \
                                    ss.artist_id AS artist_id, \
                                    se.sessionId AS session_id, \
                                    se.location AS location, \
                                    se.userAgent AS user_agent \
                            FROM staging_events se \
                            JOIN staging_songs ss ON (se.song=ss.title) \
                            WHERE se.page='NextSong';""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create ]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
