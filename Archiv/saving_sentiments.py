import psycopg2
import pandas as pd
import numpy as np
from openai import OpenAI
import json
import sys


db_config = {
    "dbname": "reviews_db",
    "user": "scraping_user",
    "password": "Passwort123",
    "host": "localhost",
    "port": 5432
}

try:
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor()

except Exception as e:
    print(f"Fehler bei der Verbindung zur Datenbank: {str(e)}")


# read csv file with all information from the sentiment analysis 
try:
    sentiment_csv = pd.read_csv("/home/ubuntu/scraping/merged_drop.csv")
except Exception as e:
    print(f"Couldn't find the csv file")
    sys.exit(1)



### load & prepare the data
# restaurant basics to get the restaurant_ids
try:
    query_general = """
        SELECT review_id
        FROM reviews_subcategories
        WHERE restaurant_id IN (
            SELECT restaurant_id
            FROM restaurant_basics
            WHERE city_id = '0'
        );
        """

    cursor.execute(query_general)
    rows = cursor.fetchall()

    # Check, ob überhaupt Daten zurückgekommen sind
    if rows:
        columns = [desc[0] for desc in cursor.description]
        restaurants = pd.DataFrame(rows, columns=columns)
        print(f"Got {len(restaurants)} restaurant IDs that are in Tübingen.")
        restaurants.to_csv("/home/ubuntu/test_laura/rev_tue.csv")

    else:
        restaurants = pd.DataFrame()
        print("No matching restaurant IDs found.")

except Exception as e:
    print(f"Failed to get the data: {e}")


data_to_save = pd.merge(restaurants, sentiment_csv, on="review_id", how="left")
# data_to_save["rating_food"] = data_to_save["rating_food"].fillna(-1).astype(int)
# data_to_save["rating_service"] = data_to_save["rating_service"].fillna(-1).astype(int)
# data_to_save["rating_atmosphere"] = data_to_save["rating_atmosphere"].fillna(-1).astype(int)


data_to_save["rating_food"] = data_to_save["rating_food"].replace(0, None )
data_to_save["rating_service"] = data_to_save["rating_service"].replace(0, None )
data_to_save["rating_atmosphere"] = data_to_save["rating_atmosphere"].replace(0, None )
data_to_save.to_csv("/home/ubuntu/test_laura/sentiments_tue.csv")


# Saving the data in the database
update_query = f"""
    UPDATE reviews_subcategories 
    SET rating_food = %s, 
        rating_service = %s, 
        rating_atmosphere = %s
    WHERE review_id = %s;
"""

rows_to_update = [
    (row['rating_food'], row['rating_service'], row['rating_atmosphere'], row['review_id'])
    for row in data_to_save.to_dict(orient='records')
]

try:
    cursor.executemany(update_query, rows_to_update)
    connection.commit()
    print(f"Successfully updated {len(rows_to_update)} rows.")
except Exception as e:
    print(f"Error updating table 'reviews_subcategories': {str(e)}")
    connection.rollback()


if cursor:
    cursor.close()
if connection:
    connection.close()

