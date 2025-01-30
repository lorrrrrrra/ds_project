import psycopg2
import pandas as pd
from openai import OpenAI
import json


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




# Function to extract rating
def extract_rating(response):
    try:
        content = json.loads(response['body']['choices'][0]['message']['content'])
        return content.get("rating")  # Return as an integer
    except Exception as e:
        print(f"Error parsing response: {e}")
        return None  # Return None if parsing fails

def handle_na_values(dataframe, columns):
    # Ersetze alle NA-Werte in den angegebenen Spalten durch None
    for column in columns[1:]:  # Überspringe 'review_id'
        dataframe[column] = dataframe[column].apply(lambda x: None if pd.isna(x) else x)
    return dataframe

def insert_data_into_table(connection, cursor, table_name, dataframe, columns):
    try:
        # Erstelle das Insert-Statement dynamisch basierend auf den Spalten
        update_query = f"""
            UPDATE {table_name}
            SET {', '.join([f"{col} = %s" for col in columns[1:]])}
            WHERE review_id = %s;
        """
        # Bereite die Daten aus dem DataFrame vor
        rows_to_insert = dataframe[columns].values.tolist()
        #print(f"Inserting rows: {rows_to_insert}")  # Check what is being inserted
        
        # Füge alle Zeilen auf einmal in die Tabelle ein
        cursor.executemany(update_query, rows_to_insert)
        connection.commit()  # Änderungen speichern
        print(f"Daten erfolgreich in die Tabelle '{table_name}' eingefügt!")
        print(f"Committed {len(rows_to_insert)} rows to the database")
    
    except Exception as e:
        print(f"Fehler beim Einfügen in Tabelle '{table_name}': {str(e)}")
        connection.rollback()  # Änderungen zurücksetzen

try: 
    batches_openai = pd.read_csv("batches_bobo_atmosphereOpenAI.csv")
    print("CSV wurde erfolgreich eingelesen!")
except Exception as e:
    print("Fehler beim Einlesen der CSV:", e)


# strip all whitespace from the values
batches_to_keys = batches_openai.applymap(lambda x: x.strip() if isinstance(x, str) else x)
batches_to_keys.head(3)

atmosphere_ratings = pd.DataFrame(columns=["review_id", "rating_atmosphere"])
food_ratings = pd.DataFrame(columns=["review_id", "rating_food"])
service_ratings = pd.DataFrame(columns=["review_id", "rating_service"])


# Inside the loop that processes each batch
for index, row in batches_to_keys.iterrows():
    batch_job_id = row['batch_job_id']
    api_key = row['api_key']
    batch_file_name = row['batch_file_name']  # Contains category name

    # Create OpenAI client
    client = OpenAI(api_key=api_key)

    # Determine category from batch_file_name
    if "food" in batch_file_name.lower():
        category_column = "rating_food"
    elif "atmosphere" in batch_file_name.lower():
        category_column = "rating_atmosphere"
    elif "service" in batch_file_name.lower():
        category_column = "rating_service"
    else:
        print(f"Unknown category in file: {batch_file_name}")
        continue  # Skip if category is unknown

    try:
        # Retrieve batch results file ID
        result_file_id = client.batches.retrieve(batch_job_id).output_file_id

        # Download the results file
        result_content = client.files.content(result_file_id).content
        result_file_name = "batch_results.jsonl"

        with open(result_file_name, 'wb') as file:
            file.write(result_content)  # Save to file

        # Parse the results
        results = []
        with open(result_file_name, 'r') as file:
            for line in file:
                results.append(json.loads(line.strip()))
        results = pd.DataFrame(results)

        # Apply function to extract ratings
        ratings = results["response"].apply(extract_rating)

        # Create a new DataFrame with the subratings
        results_df = pd.DataFrame(ratings.tolist())

        # Combine custom_id and extracted category sentences
        results_df['custom_id'] = results['custom_id']
        # Rename custom_id to review_id
        results_df.rename(columns={"custom_id": "review_id"}, inplace=True)

        # Add the ratings to the appropriate column based on the category
        results_df[category_column] = ratings

        # Ensure ratings are saved as integers (without filling NaNs)
        results_df[category_column] = results_df[category_column].astype('Int64')  # Use 'Int64' to handle NaNs as integers

        table = "reviews_subcategories"

        # Append to the correct DataFrame based on category
        if category_column == "rating_food":
            food_ratings = pd.concat([food_ratings, results_df[['review_id', 'rating_food']]], ignore_index=True)
            try:
                columns_food = ["review_id", "rating_food"]
                food_ratings= handle_na_values(food_ratings, columns_food)
                insert_data_into_table(connection, cursor, table, food_ratings, columns_food)
                print(f"Saved food ratings from batch {index}")
            except Exception as e:
                print(f"Error occurred while inserting data: {e}")
        elif category_column == "rating_service":
            service_ratings = pd.concat([service_ratings, results_df[['review_id', 'rating_service']]], ignore_index=True)
            try:
                columns_service = ["review_id", "rating_service"]
                service_ratings = handle_na_values(service_ratings, columns_service)
                insert_data_into_table(connection, cursor, table, service_ratings, columns_service)
                print(f"Saved service ratings from batch {index}")
            except Exception as e:
                print(f"Error occurred while inserting data: {e}")
        elif category_column == "rating_atmosphere":
            atmosphere_ratings = pd.concat([atmosphere_ratings, results_df[['review_id', 'rating_atmosphere']]], ignore_index=True)
            try:
                columns_atmosphere = ["review_id", "rating_atmosphere"]
                atmosphere_ratings = handle_na_values(atmosphere_ratings, columns_atmosphere)
                insert_data_into_table(connection, cursor, table, atmosphere_ratings, columns_atmosphere)
                print(f"Saved atmosphere ratings from batch {index}")
            except Exception as e:
                    print(f"Error occurred while inserting data: {e}")

    except Exception as e:
        print(f"Error processing batch {batch_job_id}: {e}")

