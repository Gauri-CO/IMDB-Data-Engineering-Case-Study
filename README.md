# IMDb-Data-Engineering-Case-Study
Develop an ETL pipeline that extracts the top 50 movies and associated metadata from the list of "Top Rated English Movies by Genre" from IMDb.

![IMDB_Image](https://user-images.githubusercontent.com/75573079/143353712-7a045a8e-af12-4888-a88d-394e9278f2e2.jpg)

**Project Objective
To acquire and operationalize data relating to the movie industry in the U.S.
As part of this project I architected and developed an ETL pipeline that extracts the top 50 movies and
associated metadata from the list of "Top Rated English Movies by Genre".

Link : https://www.imdb.com/feature/genre?ref_=fn_asr_ge

**Design Constraints
Language is python
File type is compressed csv
Database is Postgres

**Requirements
1. Develop an ETL solution that scrapes the top 50 movies of each genre into local raw data lake.
2. Develop an ETL pipeline that extracts raw data, performs necessary transformations and loads into
database.

**Project Deliverables

I. Python Source Code (The source code files are uploaded in the repository)

   WebScraping.py - This script peforms the task of web scraping  by using requests and BeautifulSoup libraries.
   The output of this script are four transformed files in compressed csv format which are saved in local data lake.
   
   fact_movies_data.csv.gz
   genre_data.csv.gz
   director_data.csv.gz
   certificate_data.csv.gz
   
   DB_DataLoad_Genre_Dim.py - Load genre_data.csv.gz to target dimension table GENRE_DIM.
   
   DB_DataLoad_Certificate_Dim.py - Load certificate_data.csv.gz to target dimension table CERTIFICATE_DIM.
   
   DB_DataLoad_Director_Dim.py - Load director_data.csv.gz to target dimension table DIRECTOR_DIM.
   
   DB_DataLoad_Fact.py - Load fact_movies_data.csv.gz to target FACT table IMDB_MOVIES_FACT.
   
   Logging is available for all five scripts in Logfile.txt uploaded in repository
  
   Validation.py - Testing and Validation Script
  
II. Docker For Postgres : Docker_Postgres_Setup.docx
  
III. ER Diagram:
  
  ![IMDb_ER_Diagram drawio (1)](https://user-images.githubusercontent.com/75573079/143365498-08deb780-c664-4437-b95b-f4027b76f203.png)

IV. System Architecture:
  
  ![Untitled Diagram drawio](https://user-images.githubusercontent.com/75573079/143365567-b06d82c8-7d99-4731-bd0e-eab34990998b.png)

 V. Query Resultset from Postgres:
   
select rank , movie_title , genre_name ,director_name , year,certificate_code, genre_url
from imdb_movies_fact  inner join genre_dim on imdb_movies_fact.genre_id=genre_dim.genre_id
inner join certificate_dim on imdb_movies_fact.certificate_id=certificate_dim.certificate_id
inner join director_dim on imdb_movies_fact.director_id=director_dim.director_id
order by  genre_name desc,rank;

   
   
 ![image](https://user-images.githubusercontent.com/75573079/143441597-ccf6249d-0450-47be-bf6c-4dd90b2f3cc4.png)

  
  



