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
import threading
import concurrent.futures

# configuration details for the postgresql database on the ubuntu server
db_config = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
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
        WHERE city_id > 0
        AND review_id > 73816;
        """

        cursor.execute(query_general)
        rows = cursor.fetchall()

        # Spaltennamen aus der Tabelle abrufen
        columns = [desc[0] for desc in cursor.description]

        # DataFrame erstellen
        data = pd.DataFrame(rows, columns=columns)
        data.to_csv("/home/ubuntu/bobo/OpenAI_bobo_test.csv", index=False)

        print("Dataframe with reviews general was constructed")
    except Exception as e:
        print("Error while accessing the data", e)

data = data.dropna(subset=['food_sentences'])

def split_dataset_into_subsets(dataframe, max_rows_per_subset=10000):
    subsets = []
    num_rows = len(dataframe)
    
    for start in range(0, num_rows, max_rows_per_subset):
        end = start + max_rows_per_subset
        subset = dataframe.iloc[start:end]
        subsets.append(subset)
    
    return subsets

# split the dataset into subsets
subsets = split_dataset_into_subsets(data)


# Define the system prompt for topic extraction
sentiment_prompt = (
        "Rate the sentiment of the following sentences on a scale of 1 to 5, "
        "where 1 is 'very bad' and 5 is 'very good'. Return the result as a JSON object with the key 'rating'. "
        "Only include the JSON object in your response.\n\n"
        "Sentences: {sentences}\n\n"
        "JSON:"
    )

# Define a function to create tasks for batch processing
def create_batch(data):
    tasks = []
    for index, row in data.iterrows():
        sentences = row[f'food_sentences']
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



# Define the directory where you want to save the files
output_dir = "/home/ubuntu/bobo/"

# read the data from the csv file
csv_file_path = os.path.join(output_dir, "batches_bobo_OpenAI.csv")


def create_client(api_key):
    """
    Creates and returns an OpenAI client for the given API key.
    """
    return OpenAI(api_key=api_key)

import concurrent.futures
import time
import os
import json
import csv

def wait_for_batches_to_complete_and_continue(api_key, batch_ids, client, next_batch_callback):
    """
    Waits for the current batches of a specific API key to complete and triggers the next batch.
    """
    while True:
        all_completed = True
        for batch_id in batch_ids:
            batch = client.batches.retrieve(batch_id)
            if batch.status != 'completed':  # Access 'status' as an attribute, not as a dictionary key
                all_completed = False
                break
        if all_completed:
            print(f"All batches {batch_ids} for API key {api_key} are completed!")
            next_batch_callback(api_key)  # Trigger the next batch processing
            break
        time.sleep(10)
def process_batches_for_key(api_key, subsets, output_dir, csv_file_path, processed_subsets, start_index=0):
    client = create_client(api_key)
    batch_ids = []
    total_subsets = len(subsets)

    while start_index < total_subsets:
        # Process the next subset
        subset_index = start_index
        start_index += 1  # Move to the next subset

        if subset_index >= total_subsets:
            break  # No more subsets available

        # Skip already processed subsets
        if subset_index in processed_subsets:
            continue
        processed_subsets.add(subset_index)

        subset = subsets[subset_index]
        batch_file_name = os.path.join(output_dir, f"batch_bobo_sentiment_analysis{subset_index + 1}.jsonl")
        subset_tasks = create_batch(subset)

        with open(batch_file_name, 'w') as file:
            for task in subset_tasks:
                file.write(json.dumps(task) + '\n')

        print(f"Saved tasks for Subset {subset_index + 1} to {batch_file_name}")
        batch_file = client.files.create(file=open(batch_file_name, "rb"), purpose="batch")
        batch_job = client.batches.create(
            input_file_id=batch_file.id,
            endpoint="/v1/chat/completions",
            completion_window="24h"
        )
        print(f"Batch job started for Subset {subset_index + 1}: {batch_job.id}")
        batch_ids.append(batch_job.id)

        # Save the batch information to the CSV file
        with open(csv_file_path, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["api_key", "batch_file_name", "batch_job_id"])
            writer.writerow({
                "api_key": api_key,
                "batch_file_name": batch_file_name,
                "batch_job_id": batch_job.id
            })

        # Wait for the batch to complete before processing the next one
        wait_for_batches_to_complete_and_continue(api_key, batch_ids, client, lambda key: None)

def process_batches_for_all_keys(api_keys, subsets, output_dir, csv_file_path, start_index=0):
    processed_subsets = set()  # Set to track processed subsets
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for api_key in api_keys:
            futures.append(executor.submit(process_batches_for_key, api_key, subsets, output_dir, csv_file_path, processed_subsets, start_index))
        
        # Wait for all futures to finish
        for future in futures:
            future.result()  # Will raise an exception if the thread raised one

# Example API keys and call to process_batches_for_all_keys function

api_keys = ["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", 
            "sk-aCxLYPM7ksEW5hRC0p80hQUs0LGuw5SQGSFgL1URxcT3BlbkFJG7J7YTe0SluXJZtU1ZMEr_y2VmtsZHIbn1nBsapmsA",
            "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",
            "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",
            "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",
            "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"]

# Call the function to process the batches
process_batches_for_all_keys(api_keys, subsets, output_dir, csv_file_path, start_index=0)
