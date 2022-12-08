import configparser


# CONFIG
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
LOG_DATA=config.get('S3','LOG_DATA')
ARN=config.get('IAM_ROLE','ARN')
SONG_DATA= config.get('S3','SONG_DATA')
LOG_JSONPATH= config.get('S3','LOG_JSONPATH')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = 'DROP TABLE IF EXISTS artists'
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE if not exists staging_events(
        artist_name text,
        auth text,
        first_name text,
        gender varchar(5),
        item_in_Session int,
        last_name text,
        length float,
        level text,
        location text,
        method text,
        page text,
        registration text,
        session_id BIGINT distkey sortkey,
        song_name text,
        status text,
        ts BIGINT ,
        agent text,
        user_id text 
        )
""")

staging_songs_table_create = (""" 
    CREATE TABLE if not exists staging_songs(
        num_songs int ,
        artist_id text,
        artist_latitude text,
        artist_longitude text,
        artist_location text,
        artist_name text,
        song_id text distkey sortkey,
        title text,
        duration float,
        year int)
""")

songplay_table_create = ("""
    CREATE TABLE if not exists songplays (
        songplay_id int identity(0,1) primary key distkey sortkey,
        start_time timestamp not null ,
        user_id text not null ,
        level text not null,
        song_id text ,artist_id text  ,
        session_id text not null,
        location text not null,
        user_agent text not null)
    """)

user_table_create = ("""
    CREATE TABLE users ( 
        user_id text primary key ,
        first_name text,
        last_name text,
        gender text,
        level text UNIQUE NOT NULL)
    """)

song_table_create = (""" 
    CREATE TABLE songs (
        song_id text primary key distkey sortkey,
        title text not null,
        artist_id text not null,
        duration float not null,
        year int not null)
    """)

artist_table_create = ("""
    CREATE TABLE if not exists artists (
        artist_id text primary key distkey sortkey,
        name text not null,
        location text,
        latitude text,
        longitude text )
    """)

time_table_create = ("""
    CREATE TABLE time (
        start_time timestamp primary key distkey sortkey,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int )
""")

# STAGING TABLES


staging_events_copy = ("""
     COPY staging_events 
     from {}
     iam_role {}
     json {}
     dateformat 'auto'
     region 'us-west-2'
""").format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs 
    from {}
    iam_role {}
    json 'auto'
    region 'us-west-2'
""").format(SONG_DATA,ARN)

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time ,
        user_id ,
        level ,
        song_id ,
        artist_id ,
        session_id ,
        location ,
        user_agent)
       
    SELECT  TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' AS start_time ,
            e.user_id, 
            e.level ,
            s.song_id ,
            s.artist_id  ,
            e.session_id ,
            e.location ,
            e.agent
    FROM staging_events e
    INNER JOIN staging_songs s
            ON e.artist_name= s.artist_name 
            AND e.song_name= s.title
    WHERE user_id is not null
    AND page='NextSong'        
            
""") 

user_table_insert = ("""
    INSERT INTO users (
        user_id ,
        first_name ,
        last_name , 
        gender , 
        level )
    SELECT DISTINCT(user_id) ,
        first_name ,
        last_name , 
        gender , 
        level
    FROM staging_events
    where user_id is not null 
    AND page='NextSong' 
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        duration,
        year) 
        
    SELECT song_id,
        title,
        artist_id,
        duration,
        year
        
        
    FROM staging_songs
            
""")

artist_table_insert = ("""
    INSERT INTO artists (
            artist_id,
            name ,
            location , 
            latitude , 
            longitude  )
        
    SELECT  DISTINCT(artist_id),
            artist_name ,
            artist_location , 
            artist_latitude , 
            artist_longitude
    
    FROM    staging_songs
            
""")


time_table_insert = (""" 
    INSERT INTO time (
        start_time , 
        hour ,
        day ,
        week ,
        month ,
        year , 
        weekday ) 
   
   SELECT   DISTINCT(TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second') AS start_time,
            EXTRACT(hour from start_time),
            EXTRACT(day from start_time),
            EXTRACT(week from start_time),
            EXTRACT(month from start_time),
            EXTRACT(year from start_time),
            EXTRACT(weekday from start_time)
            
    FROM staging_events e
    WHERE page='NextSong'         
            
            
    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
