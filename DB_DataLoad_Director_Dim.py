import psycopg2
import csv
import logging
from datetime import datetime
import gzip

# Control logging levels:
logging.basicConfig(level=logging.INFO)

# Variable Initialization
conn = None
dir_rows_deleted = 0
dir_rows_inserted = 0

# Extract Current Date for Processing
date = str(datetime.now())[0:11].replace("-", "")
tm_id = int(date)

try:
    # Connecting to Postgresql
    conn = psycopg2.connect("host='localhost' port='5432' dbname='postgres' user='postgres' password='Raju#12345'")
    cur = conn.cursor()

    # Delete if any records exists in director_dim table for complete refresh of data
    logging.info(f"{datetime.now()}: Director Dim Load Process")
    cur.execute("DELETE from director_dim ")
    dir_rows_deleted = cur.rowcount
    logging.info(f"Total rows deleted: {dir_rows_deleted}")
    conn.commit()

    # Reading director_data.csv.gz file to load into director_dim table
    with gzip.open('director_data.csv.gz', 'rt') as dir_f:
        dir_reader = csv.reader(dir_f)
        next(dir_reader)

        try:
            for row in dir_reader:
                cur.execute(
                    "INSERT INTO director_dim values (%s,%s)", row
                )
                conn.commit()
                dir_rows_inserted += 1
            logging.info(f"No. of Director Rows Inserted : {dir_rows_inserted}")
        except Exception as e:
            logging.error(f"Director Dim Load Error was {e}")

    # Closing Database Connection
    conn.close()

except(Exception, psycopg2.DatabaseError) as e:
    logging.error(f"Error was {e}")

finally:
    if conn is not None:
        conn.close()
