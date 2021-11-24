import psycopg2
import csv
import logging
from datetime import datetime
import gzip

# Control logging levels:
logging.basicConfig(level=logging.INFO)

# Variable Initialization
conn = None
genre_rows_deleted = 0
genre_rows_inserted = 0

try:
    # Connecting to Postgresql
    conn = psycopg2.connect("host='localhost' port='5432' dbname='postgres' user='postgres' password='Raju#12345'")
    cur = conn.cursor()

    # Delete if any records exists in genre_dim table for complete refresh of data
    logging.info(f"{datetime.now()}: Genre Dim Load Process")
    cur.execute("DELETE from genre_dim ")
    genre_rows_deleted = cur.rowcount
    logging.info(f"Total rows deleted: {genre_rows_deleted}")
    conn.commit()

    # Reading genre_data.csv.gz file to load into genre_dim table
    with gzip.open('genre_data.csv.gz', 'rt') as genre_f:
        genre_reader = csv.reader(genre_f)
        next(genre_reader)

        try:
            for row in genre_reader:

                cur.execute(
                    "INSERT INTO genre_dim values (%s,%s,%s)", row
                )
                conn.commit()
                genre_rows_inserted += 1
            logging.info(f"No. of Genre Rows Inserted : {genre_rows_inserted}")
        except Exception as e:
            logging.error(f"Genre Dim Load Error was {e}")

    # Closing Database Connection
    conn.close()

except(Exception, psycopg2.DatabaseError) as e:
    logging.error(f"Error was {e}")

finally:
    if conn is not None:
        conn.close()
