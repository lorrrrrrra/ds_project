import pandas as pd
import psycopg2
import sys
from openai import OpenAI
import pandas as pd
import re
from collections import defaultdict
from pydantic import BaseModel
from typing import List
import numpy as np
import os
import json
import csv
import time

# API key for OpenAI
openai_api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(
  api_key=openai_api_key
)

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
        SELECT rs.*, rb.city_id
        FROM reviews_subcategories rs
        JOIN restaurant_basics rb
        ON rs.restaurant_id = rb.restaurant_id
        WHERE city_id = 0;

        """

        cursor.execute(query_general)
        rows = cursor.fetchall()

        # Spaltennamen aus der Tabelle abrufen
        columns = [desc[0] for desc in cursor.description]

        # DataFrame erstellen
        data = pd.DataFrame(rows, columns=columns)
        #df_reviews_general.to_csv("/home/ubuntu/OpenAI/OpenAI_test.csv", index=False)

        print("Dataframe with reviews general was constructed")
    except Exception as e:
        print("Error while accessing the data", e)

# Load the data
#data = pd.read_csv("/home/ubuntu/OpenAI/extracted_category_subset.csv")
#data = data.head(10)

# Define the directory where you want to save the files
output_dir = "/home/ubuntu/OpenAI/"

# Save the data from the CSV file
csv_file_path = os.path.join(output_dir, "batches_sentiment_OpenAI.csv")

def split_dataset_into_subsets(dataframe, max_rows_per_subset=15000):
    subsets = []
    num_rows = len(dataframe)
    
    for start in range(0, num_rows, max_rows_per_subset):
        end = start + max_rows_per_subset
        subset = dataframe.iloc[start:end]
        subsets.append(subset)
    
    return subsets

def wait_for_batch_to_complete(batch_id, client):
    """
    Poll OpenAI for batch status and wait until the provided batch ID is completed.
    """
    while True:
        batch_status = client.batches.retrieve(batch_id)  # Retrieve the batch object
        if batch_status.status == "completed":  # Access 'status' as an attribute
            print(f"Batch {batch_id} completed.")
            break
        elif batch_status.status == "failed":  # Access 'status' as an attribute
            print(f"Batch {batch_id} failed.")
            break
        else:
            print(f"Waiting for batch {batch_id} to complete...")
            time.sleep(60)  # Wait 60 seconds before checking again


def process_category(data, category, client):
    """
    Process subsets of data for a specific category and submit them sequentially to OpenAI.
    """
    subsets = split_dataset_into_subsets(data.dropna(subset=[f"{category}_sentences"])[["review_id", f"{category}_sentences"]])

    # Define the system prompt
    sentiment_prompt = (
        "Rate the sentiment of the following sentences on a scale of 1 to 5, "
        "where 1 is 'very bad' and 5 is 'very good'. Return the result as a JSON object with the key 'rating'. "
        "Only include the JSON object in your response.\n\n"
        "Sentences: {sentences}\n\n"
        "JSON:"
    )

    def create_batch(data):
        tasks = []
        for index, row in data.iterrows():
            sentences = row[f'{category}_sentences']
            review_id = row['review_id']
            task = {
                "custom_id": f"{review_id}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": "gpt-4o-mini-2024-07-18",
                    "temperature": 0.1,
                    "response_format": {
                        "type": "json_object"
                    },
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are a sentiment analysis expert specializing in restaurant reviews."
                        },
                        {
                            "role": "user",
                            "content": sentiment_prompt.format(sentences=sentences)
                        }
                    ],
                }
            }
            tasks.append(task)
        return tasks

    for i, subset in enumerate(subsets):
        tasks = create_batch(subset)
        batch_file_name = os.path.join(output_dir, f"batch_sentiment_analysis_{category}_{i}.jsonl")
        
        # Save tasks to .jsonl file
        with open(batch_file_name, 'w') as file:
            for task in tasks:
                file.write(json.dumps(task) + '\n')
        print(f"Saved tasks for {category} subset {i} to {batch_file_name}")

        try:
            # Upload the batch file
            batch_file = client.files.create(
                file=open(batch_file_name, "rb"),
                purpose="batch"
            )
            print(f"Batch file uploaded: {batch_file.id}")

            # Create a batch job for the uploaded file
            batch_job = client.batches.create(
                input_file_id=batch_file.id,
                endpoint="/v1/chat/completions",
                completion_window="24h"
            )
            print(f"Batch job started: {batch_job.id}")

            # Append the information to the CSV file
            with open(csv_file_path, 'a', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=["api_key", "batch_file_name", "batch_job_id"])
                writer.writerow({
                    "api_key": client.api_key,
                    "batch_file_name": batch_file_name,
                    "batch_job_id": batch_job.id
                })

            # Wait for the batch to complete before moving to the next subset
            wait_for_batch_to_complete(batch_job.id, client)

        except Exception as e:
            print(f"Error uploading {batch_file_name} or creating batch job: {e}")

# Process the categories sequentially
categories = ["service", "atmosphere"]
for category in categories:
    print(f"Processing category: {category}")
    process_category(data, category, client)

