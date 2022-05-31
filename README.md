# Readme.md for sparkifydb ETL pipeline to populate Postgres DB

## Purpose:
The purpose of this database is to provide datasets that enable answering questions about user behavior in the Sparkify app (especially user song 
listening behavior). To do so we ingest the app logs of user behavior and song metadata into a PostgreSQL database, 'sparkifydb', from where we can run
queries to answer such questions.

## An explanation of the files in the repository:
* create_tables.py: drops and creates your tables. Run this file to reset your (empty) tables before each time you run your ETL scripts
* etl.ipynb: reads and processes a single file from song_data and log_data input files and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables
* etl.py: reads and processes files from song_data and log_data and loads them into your tables
* sql_queries.py: contains all of the sql queries, and is imported into the three files above (those files run these queries)
* test.ipynb: running this displays the first few rows of each table to let you check your database at any given time for testing purposes
* sparkifydb_erd.png: this is an image file that provides an easy-to-understand overview of the database design. This image gets re-created each time the db is populated (via the etl.py script), so changes to the db will be reflected in this image

## How to run the Python scripts to create and populate the 'sparkifydb' database:
The full ETL flow only consists of two steps:
1. Run create_tables.py to drop and create all tables
2. Run etl.py to populate tables with data from the song_data and log_data files and create the sparkifydb_erd.png image to provide an overview of the database design

The other files are all either supporting files (e.g. sql_queries.py stores queries that the .py scripts use) or are for testing (test.ipynb and etl.ipynb are useful for testing new/changed functionality before implementing in the production files)

## Database schema design and ETL pipeline:
We've designed the database to have one fact table to store user song listening activity and a handful of dimension tables to store relevant entities. The fact table, 'songplays', is relatively normalized, so users will need to join to relevant tables to pull in most song/artist/user details.

Fact Table(s):
* songplays: stores the following for each user listening activity: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dim Tables:
* songs: user_id, first_name, last_name, gender, level
* artists: artist_id, name, location, latitude, longitude
* users: user_id, first_name, last_name, gender, level
* time: start_time, hour, day, week, month, year, weekday

![alt text](https://github.com/mimoyer21/udacity-sparkifydb/blob/main/sparkifydb_erd.png?raw=true) 

## Example queries and results for song play analysis:
(to be filled in with more examples later if desired)

Total number of song plays by all users during a given time period (in this case, 2018):
```sql
select count(*) 
from songplays sp 
join time t 
    on sp.start_time = t.start_time 
where t.year = 2018;
```
