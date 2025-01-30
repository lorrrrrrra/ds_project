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

# getting all the restaurant ids
if connection:
    try:
        query_general = """
        SELECT restaurant_id
        FROM restaurant_general;
        """

        cursor.execute(query_general)
        rows = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]

        restaurants = pd.DataFrame(rows, columns=columns)

        print("Dataframe with restaurant additional was constructed")
    except Exception as e:
        print("Error while accessing the data", e)


if connection:
    try:
        query_general = """
        SELECT distinct(dining_price_range)
        FROM reviews_additional;
        """

        cursor.execute(query_general)
        rows = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]

        unique_price_ranges = pd.DataFrame(rows, columns=columns)

        print("Dataframe with restaurant additional was constructed")
    except Exception as e:
        print("Error while accessing the data", e)


unique_values = unique_price_ranges["dining_price_range"].dropna().unique()  # NaN entfernen
price_mapping = {val: idx for idx, val in enumerate(unique_values)}

reverse_price_mapping = {v: k.replace(" ", "").replace("€", "").replace("-", "") for k, v in price_mapping.items()}

def map_avg_price_to_category(avg_price):
    closest_price = min(price_mapping.values(), key=lambda x: abs(x - avg_price))
    return reverse_price_mapping[closest_price]




for index, row in restaurants.iterrows():
    restaurant_id = row['restaurant_id']

    print(f"Getting data for: {restaurant_id}")

    try:
        # getting data from the database for each restaurant
        query_reviews = f"""
            SELECT dining_price_range
            FROM reviews_additional
            WHERE restaurant_id = '{restaurant_id}';
        """
        
        cursor.execute(query_reviews)
        rows = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]

        # DataFrame erstellen
        price_range = pd.DataFrame(rows, columns=columns)

    except Exception as e:
        print(f"There was a failure to get price ranges for restaurant {restaurant_id}: {e}")
        continue

    if not price_range.empty:
        try:
            # NaN und leere Strings entfernen
            price_range = price_range.dropna(subset=["dining_price_range"])  
            price_range = price_range[price_range["dining_price_range"].str.strip() != ""]
            # print(len(price_range))
            
            # Mapping durchführen
            price_range["mapped_price_range"] = price_range["dining_price_range"].map(price_mapping)
            
            # Überprüfen, ob es gültige Werte gibt
            if price_range["mapped_price_range"].notna().sum() > 0:
                avg_price = price_range["mapped_price_range"].mean()
                avg_price_rounded = round(avg_price)
                price_category = map_avg_price_to_category(avg_price_rounded)
                print(f"Average price for {restaurant_id}: {price_category}")
            else:
                print(f"No valid price data for {restaurant_id}")
                avg_price_rounded = None  # Alternativ: avg_price_rounded = 0

        except Exception as e:
            print(f"Error processing price data for {restaurant_id}: {e}")
        
        try:
            update_query = f"""
                    UPDATE restaurant_general
                    SET rating_price = '{price_category}'
                    WHERE restaurant_id = '{restaurant_id}';
                """
            cursor.execute(update_query)
            connection.commit()

            print(f"Updated price rating for restaurant {restaurant_id}: {price_category} € {avg_price_rounded}")

        except Exception as e:
            print(f"Error writing avg price range in table {e}")

    