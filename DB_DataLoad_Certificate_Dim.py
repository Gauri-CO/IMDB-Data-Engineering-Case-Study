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
certificate_rows_deleted = 0
certificate_rows_inserted = 0


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

    # Delete if any records exists in certificate_dim table for complete refresh of data
    logging.info(f"{datetime.now()}: Certificate Dim Load Process")
    cur.execute("DELETE from certificate_dim ")
    certificate_rows_deleted = cur.rowcount
    logging.info(f"Total rows deleted: {certificate_rows_deleted}")
    conn.commit()

    # Reading certificate_data.csv.gz file to load into certificate_dim table
    with gzip.open('certificate_data.csv.gz', 'rt') as certificate_f:
        certificate_reader = csv.reader(certificate_f)
        next(certificate_reader)

        try:
            for row in certificate_reader:
                cur.execute(
                    "INSERT INTO certificate_dim values (%s,%s)", row
                )
                conn.commit()
                certificate_rows_inserted += 1
            logging.info(f"No. of Certificate Rows Inserted : {certificate_rows_inserted}")
        except Exception as e:
            logging.error(f"Certificate Dim Load Error was {e}")

    # Closing Database Connection
    conn.close()

except(Exception, psycopg2.DatabaseError) as e:
    logging.error(f"Error was {e}")

finally:
    if conn is not None:
        conn.close()
