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
        SELECT *
        FROM reviews_general;
        """

        cursor.execute(query_general)
        rows = cursor.fetchall()

        # Spaltennamen aus der Tabelle abrufen
        columns = [desc[0] for desc in cursor.description]

        # DataFrame erstellen
        df_reviews_general = pd.DataFrame(rows, columns=columns)
        #df_reviews_general.to_csv("/home/ubuntu/OpenAI/OpenAI_test.csv", index=False)

        print("Dataframe with reviews general was constructed")
    except Exception as e:
        print("Error while accessing the data", e)


# preprocess data to save input tokens
# Drop rows where 'review_text' is NaN
data = df_reviews_general.dropna(subset=['review_text'])
# remove extra spaces, newlines, and tabs
data['review_text'] = data['review_text'].str.replace(r'\s+', ' ', regex=True).str.strip()
# Drop rows where the "review_text" column contains only one character
data = data[data["review_text"].str.len() > 10]

print(len(data))


def split_dataset_into_subsets(dataframe, max_rows_per_subset=5000):
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
topic_extraction_prompt = '''
You are an expert at structured data extraction. Extract sentences from restaurant reviews that mention the following topics: food, service, atmosphere, and price.
Return the results in the following JSON format:

{
    "food_sentences": string[], 
    "service_sentences": string[], 
    "atmosphere_sentences": string[], 
    "price_sentences": string[]
}
'''

# Define a function to create tasks for batch processing
def create_batch_tasks(data):
    tasks = []
    
    for index, row in data.iterrows():
        review_text = row['review_text']
        review_id = row['review_id']  # Access the review_id from the DataFrame
        
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
                        "content": topic_extraction_prompt
                    },
                    {
                        "role": "user",
                        "content": review_text
                    }
                ],
            }
        }
        tasks.append(task)
    
    return tasks



# Define the directory where you want to save the files
output_dir = "/home/ubuntu/OpenAI/"

# read the data from the csv file
csv_file_path = os.path.join(output_dir, "batches_OpenAI.csv")


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
        # Process the first subset
        subset_1_index = start_index
        subset_2_index = start_index + 1
        start_index += 2

        if subset_1_index >= total_subsets:
            break  # No more subsets available

        # Skip already processed subsets
        if subset_1_index in processed_subsets:
            continue
        processed_subsets.add(subset_1_index)

        subset_1 = subsets[subset_1_index]
        batch_file_name_1 = os.path.join(output_dir, f"batch_topic_recognition{subset_1_index + 1}.jsonl")
        subset_tasks_1 = create_batch_tasks(subset_1)

        with open(batch_file_name_1, 'w') as file:
            for task in subset_tasks_1:
                file.write(json.dumps(task) + '\n')

        print(f"Saved tasks for Subset {subset_1_index + 1} to {batch_file_name_1}")
        batch_file_1 = client.files.create(file=open(batch_file_name_1, "rb"), purpose="batch")
        batch_job_1 = client.batches.create(
            input_file_id=batch_file_1.id,
            endpoint="/v1/chat/completions",
            completion_window="24h"
        )
        print(f"Batch job started for Subset {subset_1_index + 1}: {batch_job_1.id}")
        batch_ids.append(batch_job_1.id)

        # Process the second subset (if available)
        if subset_2_index < total_subsets and subset_2_index not in processed_subsets:
            processed_subsets.add(subset_2_index)

            subset_2 = subsets[subset_2_index]
            batch_file_name_2 = os.path.join(output_dir, f"batch_topic_recognition_{subset_2_index + 1}.jsonl")
            subset_tasks_2 = create_batch_tasks(subset_2)

            with open(batch_file_name_2, 'w') as file:
                for task in subset_tasks_2:
                    file.write(json.dumps(task) + '\n')

            print(f"Saved tasks for Subset {subset_2_index + 1} to {batch_file_name_2}")
            batch_file_2 = client.files.create(file=open(batch_file_name_2, "rb"), purpose="batch")
            batch_job_2 = client.batches.create(
                input_file_id=batch_file_2.id,
                endpoint="/v1/chat/completions",
                completion_window="24h"
            )
            print(f"Batch job started for Subset {subset_2_index + 1}: {batch_job_2.id}")
            batch_ids.append(batch_job_2.id)

        # Save the batch information to the CSV file
        with open(csv_file_path, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["api_key", "batch_file_name", "batch_job_id"])
            writer.writerow({
                "api_key": api_key,
                "batch_file_name": batch_file_name_1,
                "batch_job_id": batch_job_1.id
            })
            if subset_2_index < total_subsets:
                writer.writerow({
                    "api_key": api_key,
                    "batch_file_name": batch_file_name_2,
                    "batch_job_id": batch_job_2.id
                })

        # Wait for batches to complete
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

# Define a list of environment variable names for the API keys
api_key_env_vars = [
    "OPENAI_API_KEY_1",
    "OPENAI_API_KEY_2",
    "OPENAI_API_KEY_3",
    "OPENAI_API_KEY_4",
    "OPENAI_API_KEY_5",
    "OPENAI_API_KEY_6",
]

# Retrieve the API keys from environment variables
api_keys = [os.getenv(var) for var in api_key_env_vars if os.getenv(var) is not None]


process_batches_for_all_keys(api_keys, subsets, output_dir, csv_file_path, start_index=26)

