import psycopg2
import csv
import logging
from datetime import datetime
import gzip
import yaml

# Control logging levels:
logging.basicConfig(level=logging.INFO)

# Variable Initialization
conn = None
fact_rows_deleted = 0
fact_rows_inserted = 0

# Extract Current Date for Processing
date = int(str(datetime.now())[0:11].replace("-", ""))

# Read config.yaml file for extracting connection parameters
def load_config():
    with open("config.yaml", "r") as yamlfile:
        try:
            return yaml.load(yamlfile, Loader=yaml.FullLoader)
        except yaml.YAMLError as e:
            logging.error(f"Error : {e}")


try:

    # Extract the database connection parameters from config.yaml file
    config = load_config()
    host = config[0]["host"]
    port = config[0]["port"]
    dbname = config[0]["dbname"]
    user = config[0]["user"]
    password = config[0]["password"]

    # Connecting to Postgresql
    conn = psycopg2.connect(f"host={host} port={port} dbname={dbname} user={user} password={password}")
    cur = conn.cursor()

    # Delete if any records exists in fact table for complete refresh of data
    logging.info(f"{datetime.now()}: Fact Load Process")
    cur.execute("DELETE from imdb_movies_fact where date=%s", (date,))
    fact_rows_deleted = cur.rowcount
    logging.info(f"Total rows deleted: {fact_rows_deleted}")
    conn.commit()

    # Reading imdb_movies_stage_data.csv.gz file to load into fact table
    with gzip.open('fact_movies_data.csv.gz', 'rt') as fact_f:
        fact_reader = csv.reader(fact_f)
        next(fact_reader)

        try:
            for row in fact_reader:

                cur.execute(
                    "INSERT INTO imdb_movies_fact values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", row
                )
                conn.commit()
                fact_rows_inserted += 1
            logging.info(f"No. of Fact Rows Inserted : {fact_rows_inserted}")
        except Exception as e:
            logging.error(f"Fact Load Error was: {e}")

    # Closing Database Connection
    conn.close()

except(Exception, psycopg2.DatabaseError) as e:
    logging.error(f"Error was {e}")

finally:
    if conn is not None:
        conn.close()
