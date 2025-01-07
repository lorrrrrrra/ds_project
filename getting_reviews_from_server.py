import pandas as pd
import psycopg2
import sys


# configuration details for the postgresql database on the ubuntu server
db_config = {
    "dbname": "reviews_db",
    "user": "scraping_user",
    "password": "Passwort123",
    "host": "localhost",
    "port": 5432
}

# connecting to the database, exiting the file if it does not work
try:
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor()
    print("Connection to database was established")
except Exception as e:
    print("Connection to database not possible", e)
    sys.exit(1)



# getting the data from the database
if connection:
    # reviews general table
    try:
        # small fail, the incremented review_id didn't reset after the final test so the reviews start with id 4471
        query_general = """
        SELECT *
        FROM reviews_general
        WHERE (review_id BETWEEN 4471 AND 29470)
        OR (review_id BETWEEN 104471 AND 129470)
        OR (review_id BETWEEN 204471 AND 229470)
        OR (review_id BETWEEN 304471 AND 329470);
        """

        cursor.execute(query_general)
        rows = cursor.fetchall()

        # Spaltennamen aus der Tabelle abrufen
        columns = [desc[0] for desc in cursor.description]

        # DataFrame erstellen
        df_reviews_general = pd.DataFrame(rows, columns=columns)
        df_reviews_general.to_csv("/home/ubuntu/scraping/reviews_general_selected.csv", index=False)

        print("Dataframe with reviews general was constructed")
    except Exception as e:
        print("Error while accessing the data", e)


    # reviews additional table
    try:
        # small fail, the incremented review_id didn't reset after the final test so the reviews start with id 4471
        query_additional = """
        SELECT *
        FROM reviews_additional
        WHERE (review_id BETWEEN 4471 AND 29470)
        OR (review_id BETWEEN 104471 AND 129470)
        OR (review_id BETWEEN 204471 AND 229470)
        OR (review_id BETWEEN 304471 AND 329470);
        """

        cursor.execute(query_additional)
        rows = cursor.fetchall()

        # Spaltennamen aus der Tabelle abrufen
        columns = [desc[0] for desc in cursor.description]

        # DataFrame erstellen
        df_reviews_additional = pd.DataFrame(rows, columns=columns)
        df_reviews_additional.to_csv("/home/ubuntu/scraping/reviews_additional_selected.csv", index=False)

        print("Dataframe with reviews additional was constructed")
    except Exception as e:
        print("Error while accessing the data", e)


    finally:
        if cursor:
            cursor.close()
            print("Cursor was closed")
        if connection:
            connection.close()
            print("Connection was closed")