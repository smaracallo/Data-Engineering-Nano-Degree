songs_table_query = '''
                    SELECT DISTINCT song_id, title as song_title, artist_id, year, duration 
                    FROM songs
                    '''

artists_table_query = '''
                      SELECT DISTINCT artist_id, artist_name as name, artist_location as location, artist_latitude as lattitude, artist_longitude as longitude
                      FROM songs
                      '''

log_filter_query = '''
                    SELECT *, cast(ts/1000 as TimeStamp) as timestamp
                    FROM song_logs
                    WHERE page = "NextSong"
                   '''

users_table_query = '''
                    SELECT DISTINCT userId as user_id, firstName as first_name, lastName as last_name, gender, level
                    FROM song_logs
                    '''

time_table_query = '''
                   SELECT DISTINCT timestamp as start_time, hour(timestamp) as hour, day(timestamp) as day, weekofyear(timestamp) as week, month(timestamp) as month, 
                   year(timestamp) as year, weekday(timestamp) as weekday
                   FROM song_logs
                   '''

songplays_table_query = ''' 
                        SELECT a.ts as start_time, a.userId as user_id, a.level, b.song_id, b.artist_id, a.sessionId as session_id , a.location, 
                        a.userAgent as user_agent, year(a.timestamp) as year, month(a.timestamp) as month 
                        FROM song_logs as a 
                        INNER JOIN songplays as b on a.song = b.song_title
                        '''
