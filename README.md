# Sparkify Song Analysis ETL Pipeline
## Goals
Create a database optimized for queries on song play analysis. Potential questions to be answered by the resulting datastore:
* What time of day is an artist/song typically listened to?
* What are paid users listening to the most?
* What is the top listened to artist/song by location?
* What is an artist's most listened to song?
* What is an artist's total listens across all of their songs?

## Schema
![Star Schema](https://i.imgur.com/Sfoy2Op.png)

Data is first normalized (3NF) into the users, songs, artists, and time tables. This data is then used to create the required Fact table 'songplays' on which the team will run their queries. Analysis able to be done initially on the IDs in the fact table for performance benefits.

### Fact Table
    1. **songplays** - records in log data associated with song plays i.e. records with page `NextSong`
        - *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

### Dimension Tables
    1. **users** - users in the app
        - *user_id, first_name, last_name, gender, level*
    2. **songs** - songs in music database
        - *song_id, title, artist_id, year, duration*
    3. **artists** - artists in music database
        - *artist_id, name, location, latitude, longitude*
    4. **time** - timestamps of records in **songplays** broken down into specific units
        - *start_time, hour, day, week, month, year, weekday*


## Pipeline Steps
Transformation is required on the data coming from the existing JSON logs in order to create the dimension tables:

1. Grouping data points into dimension tables (ex. user_id with first_name)
2. Transforming the UNIX epoch timestamp into a datetime and its components (hours, months, etc) for easy access to each
3. psycopg doesn't like numpy data types and the song and artist data has an np.int64 and an np.float64 in it when read in by pandas. Using a list comprehension solves this.

As artist_id and song_id are not available on the log level, the songplays fact table requires the following query:

    SELECT songs.song_id, artists.artist_id FROM songs
    INNER JOIN artists ON songs.artist_id = artists.artist_id
    WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s;
    
Note that this WHERE statement does leave a margin for duplicates - if the same song has multiple records in the Sparkify system with different IDs this could result in an incorrect association.

Sample data is lacking in matches - the dataset I am working with found only once confirmed artist/song record for the sessions in the logs.

## Sample Queries
Number of listens by artist:

    SELECT a.name, COUNT(s.session_id) FROM songplays s JOIN artists a ON s.artist_id = a.artist_id GROUP BY a.name;
    
Listen counts by song by location:

    SELECT sp.location, s.title, COUNT(sp.session_id) FROM songplays sp JOIN songs s ON sp.song_id = s.song_id GROUP BY s.title, sp.location
    
Song listen counts by paid users:

    SELECT s.title, COUNT(sp.session_id) FROM songplays sp JOIN songs s ON sp.song_id = s.song_id  WHERE sp.level = 'paid' GROUP BY s.title