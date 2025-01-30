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




# Extract the sentences for each topic
def extract_sentences(response):
    try:
        # Parse the assistant's message content
        content = json.loads(response['body']['choices'][0]['message']['content'])
        return {
            "food_sentences": " ".join(content.get("food_sentences", [])),
            "service_sentences": " ".join(content.get("service_sentences", [])),
            "atmosphere_sentences": " ".join(content.get("atmosphere_sentences", [])),
            "price_sentences": " ".join(content.get("price_sentences", [])),
        }
    except Exception as e:
        print(f"Error parsing response: {e}")
        return {
            "food_sentences": None,
            "service_sentences": None,
            "atmosphere_sentences": None,
            "price_sentences": None,
        }
    

def insert_data_into_table(connection, cursor, table_name, dataframe, columns):
    try:
        # Erstelle das Insert-Statement dynamisch basierend auf den Spalten
        insert_query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({', '.join(['%s'] * len(columns))});
        """
        
        # Bereite die Daten aus dem DataFrame vor
        rows_to_insert = dataframe[columns].values.tolist()
        
        # Füge alle Zeilen auf einmal in die Tabelle ein
        cursor.executemany(insert_query, rows_to_insert)
        connection.commit()  # Änderungen speichern
        # print(f"Daten erfolgreich in die Tabelle '{table_name}' eingefügt!")
    
    except Exception as e:
        print(f"Fehler beim Einfügen in Tabelle '{table_name}': {str(e)}")
        connection.rollback()  # Änderungen zurücksetzen
            
            
try: 
    batches_openai = pd.read_csv("batches_OpenAI.csv")
    print("CSV wurde erfolgreich eingelesen!")
except Exception as e:
    print("Fehler beim Einlesen der CSV:", e)


# strip all whitespace from the values
batches_to_keys = batches_openai.applymap(lambda x: x.strip() if isinstance(x, str) else x)




for index, row in batches_to_keys.iterrows():
    batch_job_id = row['batch_job_id']
    api_key = row['api_key']
    
    # Create the OpenAI client
    client = OpenAI(api_key=api_key)
    
    try:
        # Get the results file ID for the batch job (assuming this comes from an API or prior step)
        result_file_id = client.batches.retrieve(batch_job_id).output_file_id
        
        # Download the results file
        result_content = client.files.content(result_file_id).content
        result_file_name = "batch_results.jsonl"
        with open(result_file_name, 'wb') as file:
            file.write(result_content)  # Save the content to a file

        # Parse the results
        results = []
        with open(result_file_name, 'r') as file:
            for line in file:
                results.append(json.loads(line.strip()))
        results = pd.DataFrame(results)

        

        # Apply the extraction to the 'response' column
        category_data = results['response'].apply(extract_sentences)

        # Create a new DataFrame with the extracted category sentences
        category_df = pd.DataFrame(category_data.tolist())

        # Combine 'custom_id' and extracted category sentences
        category_df['custom_id'] = results['custom_id']
        # Rename custom_id to review_id
        category_df.rename(columns={"custom_id": "review_id"}, inplace=True)

        print("Got results")

        #left join on category_df to get the restaurant id
        # min_review_id = min(category_df["review_id"].astype(int))
        # max_review_id = max(category_df["review_id"].astype(int))

        # query = """
        #     SELECT review_id, restaurant_id
        #     FROM reviews_general
        #     WHERE review_id BETWEEN %s AND %s
        # """
        # mapping_df = pd.read_sql_query(query, connection, params=(min_review_id, max_review_id))


        # category_df = pd.merge(category_df, mapping_df, on="review_id", how="left")

        # changing position of rows
        category_df = category_df[["review_id","food_sentences", "service_sentences", "atmosphere_sentences", "price_sentences"]]

    except Exception as e:
        print(f"Error processing batch_job_id {batch_job_id}: {e}")


    try:
        table = "reviews_subcategories"
        columns = ["review_id", "food_sentences", "service_sentences", "atmosphere_sentences", "price_sentences"]
        insert_data_into_table(connection, cursor, table, category_df, columns)

        print(f"Saved recognized subcategory sentences from batch {index}")

    except Exception as e:
        print(f"Errrrooooooor: {e}")




if cursor:
    cursor.close()
if connection:
    connection.close()