import os.path
from os import path
import logging
import psycopg2
import gzip
from datetime import datetime

# Control logging levels:
logging.basicConfig(level=logging.INFO)

# Extract Current Date for Processing
date = int(str(datetime.now())[0:11].replace("-", ""))


# Function to test if file exists or not
def fileCheck(*args):
    inpath = os.getcwd()

    try:

        logging.info("### File Check Validation ###")
        for file in args:
            filename = inpath + "\\" + file

            if path.exists(filename):
                logging.info(f"{file}  File Exists!!")
            else:
                raise Exception(f"{file} does not exists!!")

    except Exception as e:
        logging.error(f"Error : {e}")

    pass


def fileCount(filename):

    with gzip.open(filename, 'rt') as f:
        f.readline()
        return len(f.readlines())


# Function to validate file and table count
def countCheck(**kwargs):
    try:
        logging.info("### Count Check Validation ###")
        conn = psycopg2.connect("host='localhost' port='5432' dbname='postgres' user='postgres' password='Raju#12345'")
        cur = conn.cursor()
        for table, file in kwargs.items():
            if table == "imdb_movies_fact":
                cur.execute(f"select count(*) from {table} where date=%s", (date,))

            else:
                cur.execute(f"select count(*) from {table}")
            table_count = cur.fetchall()[0][0]

            filename = os.getcwd() + '\\' + file
            file_count = fileCount(filename)
            logging.info(f"{table_count} and {file_count}")
            if table_count == file_count:
                logging.info(f"{table} and {file} match")
            else:
                raise Exception(f"{table} and {file} do not match")

    except Exception as e:

        logging.error(f"Error {e}")
        pass

    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    fileCheck('genre_data.csv.gz', 'fact_movies_data.csv.gz', 'director_data.csv.gz', 'certificate_data.csv.gz',
              'imdb_movies_stage_data.csv.gz')
    countCheck(imdb_movies_fact='fact_movies_data.csv.gz',
               certificate_dim='certificate_data.csv.gz',
               genre_dim='genre_data.csv.gz',
               director_dim='director_data.csv.gz')
