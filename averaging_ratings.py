# currently just for tübingen
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



# generating a list with all restaurants from tübigen
try:
    query_general = """
        SELECT restaurant_id
        FROM restaurant_basics
        WHERE city_id = '0';
        """

    cursor.execute(query_general)
    rows = cursor.fetchall()

    # Check, ob überhaupt Daten zurückgekommen sind
    if rows:
        columns = [desc[0] for desc in cursor.description]
        restaurants = pd.DataFrame(rows, columns=columns)
        print(f"Got {len(restaurants)} restaurant IDs that are in Tübingen.")

    else:
        restaurants = pd.DataFrame()
        print("No matching restaurant IDs found.")

except Exception as e:
    print(f"Failed to get the data: {e}")




# looping over the restaurants table, getting all the ratings from the reviews_subcategories and calculating an average

for _, row in restaurants.iterrows():
    restaurant_id = row["restaurant_id"]

    try:
        query_general = f"""
            SELECT rating_food, rating_service, rating_atmosphere
            FROM reviews_subcategories
            WHERE restaurant_id = '{restaurant_id}';
            """

        cursor.execute(query_general)
        rows = cursor.fetchall()

        if rows:
            columns = [desc[0] for desc in cursor.description]
            ratings = pd.DataFrame(rows, columns=columns)

            # Calculating the averages
            # rating_food_avg = ratings["rating_food"].astype(float).mean(skipna=True)
            # rating_food_avg = None if np.isnan(rating_food_avg) else round(rating_food_avg, 1)
            # rating_service_avg = ratings["rating_service"].astype(float).mean(skipna=True)
            # rating_service_avg = None if np.isnan(rating_service_avg) else round(rating_service_avg, 1)
            # rating_atmosphere_avg = ratings["rating_atmosphere"].astype(float).mean(skipna=True)
            # rating_atmosphere_avg = None if np.isnan(rating_atmosphere_avg) else round(rating_atmosphere_avg, 1)

            # rating_food_avg = float(rating_food_avg) if not pd.isna(rating_food_avg) else None
            # rating_service_avg = float(rating_service_avg) if not pd.isna(rating_service_avg) else None
            # rating_atmosphere_avg = float(rating_atmosphere_avg) if not pd.isna(rating_atmosphere_avg) else None

            def safe_avg(series):
                mean_value = series.astype(float).mean(skipna=True)
                return float(round(mean_value, 1)) if not np.isnan(mean_value) else None

            rating_food_avg = safe_avg(ratings["rating_food"])
            rating_service_avg = safe_avg(ratings["rating_service"])
            rating_atmosphere_avg = safe_avg(ratings["rating_atmosphere"])


            # print(f"{restaurant_id}: Average food: {rating_food_avg}, Average service:  {rating_service_avg}, Average atmosphere: {rating_atmosphere_avg}")


            # Saving the averages in the database
            try:
                update_query = """
                    UPDATE restaurant_general 
                    SET rating_food = %s, 
                        rating_service = %s, 
                        rating_atmosphere = %s
                    WHERE restaurant_id = %s;
                """

                cursor.execute(update_query, (rating_food_avg, rating_service_avg, rating_atmosphere_avg, restaurant_id))
                connection.commit()

            except Exception as e:
                print(f"Error saving the averages for {restaurant_id} in the database {e}")

        else:
            restaurants = pd.DataFrame()
            print("No matching restaurant IDs found.")

    except Exception as e:
        print(f"Failed to get the data: {e}")




# closing the connection
if cursor:
    cursor.close()
if connection:
    connection.close()



