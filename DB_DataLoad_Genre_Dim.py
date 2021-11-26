import psycopg2
import csv
import logging
from datetime import datetime
import gzip
import yaml

# Control logging levels:
logging.basicConfig(level=logging.INFO)


# Read config.yaml file for extracting connection parameters
def load_config():
    with open("config.yaml", "r") as yamlfile:
        try:
            return yaml.load(yamlfile, Loader=yaml.FullLoader)
        except yaml.YAMLError as e:
            logging.error(f"Error : {e}")


if __name__ == "__main__":

    # Variable Initialization
    conn = None
    genre_rows_deleted = 0
    genre_rows_inserted = 0

    # Extract the database connection parameters from config.yaml file
    config = load_config()
    host = config[0]["host"]
    port = config[0]["port"]
    dbname = config[0]["dbname"]
    user = config[0]["user"]
    password = config[0]["password"]

    try:
        # Connecting to Postgresql
        conn = psycopg2.connect(f"host={host} port={port} dbname={dbname} user={user} password={password}")
        cur = conn.cursor()

        # Delete if any records exists in genre_dim table for complete refresh of data
        logging.info(f"{datetime.now()}: Genre Dim Load Process")
        cur.execute("delete from genre_dim ")
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
