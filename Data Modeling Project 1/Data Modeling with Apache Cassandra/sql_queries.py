# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS music_session"
user_table_drop = "DROP TABLE IF EXISTS user_session"
song_table_drop = "DROP TABLE IF EXISTS song_user"


# CREATE TABLES

music_session_create = (""" 
CREATE TABLE IF NOT EXISTS music_session (
    sessionId int, 
    itemInSession int, 
    artist text, 
    song text, 
    length float, 
    PRIMARY KEY (sessionId, itemInSession))
""")

user_session_create = ("""
CREATE TABLE IF NOT EXISTS user_session (
    user_id int, 
    session_id int, 
    itemInSession int,
    artist text, 
    song text, 
    first_name text, 
    last_name text, 
    PRIMARY KEY ((user_id, session_id), itemInSession))
""")

song_user_create = ("""
CREATE TABLE IF NOT EXISTS song_user (
    song text, 
    user_id int, 
    first_name text, 
    last_name text, 
    PRIMARY KEY (song, user_id))
""")

# INSERT RECORDS

music_session_insert = ("""
INSERT INTO music_session (sessionId, itemInSession, artist, song, length) VALUES (%s, %s, %s, %s, %s);
""")

user_session_insert = ("""
INSERT INTO user_session (user_id, session_id, itemInSession, artist, song, first_name, last_name) VALUES (%s, %s, %s, %s, %s, %s, %s);
""")

song_user_insert = ("""
INSERT INTO song_user (song, user_id, first_name, last_name) VALUES (%s, %s, %s, %s);
""")

# Verify tables exist

music_session_select = ("""
SELECT artist, song, length FROM music_session WHERE sessionid=338 AND iteminSession=4;
""")

user_session_select = ("""
SELECT artist, song, first_name, last_name FROM user_session WHERE user_id=10 AND session_id=182;
""")

song_user_select = ("""
SELECT first_name, last_name FROM song_user WHERE song='All Hands Against His Own';
""")

# QUERY LISTS

drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop]
process_tables = [{'table': 'music_session', 'create': music_session_create, 'insert': music_session_insert, 'select': music_session_select},
                  {'table': 'user_session', 'create': user_session_create, 'insert': user_session_insert, 'select': user_session_select},
                  {'table': 'song_user', 'create': song_user_create, 'insert': song_user_insert, 'select': song_user_select}]

# process_tables = [{'table': 'user_session', 'create': user_session_create, 'insert': user_session_insert, 'select': user_session_select}]