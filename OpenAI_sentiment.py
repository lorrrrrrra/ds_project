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

# Theresa 3
client = OpenAI(
  api_key="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"
)

# configuration details for the postgresql database on the ubuntu server
db_config = {
    "dbname": "reviews_db",
    "user": "scraping_user",
    "password": "Passwort123",
    "host": "localhost",
    "port": 5432
}

# # connecting to the database, exiting the file if it does not work
# try:
#     connection = psycopg2.connect(**db_config)
#     cursor = connection.cursor()
#     print("Connection to database was established")
# except Exception as e:
#     print("Connection to database not possible", e)
#     sys.exit(1)

# # getting the data from the database
# if connection:
#     # reviews general table
#     try:
#         # small fail, the incremented review_id didn't reset after the final test so the reviews start with id 4471
#         query_general = """
#         SELECT *
#         FROM reviews_general;
#         """

#         cursor.execute(query_general)
#         rows = cursor.fetchall()

#         # Spaltennamen aus der Tabelle abrufen
#         columns = [desc[0] for desc in cursor.description]

#         # DataFrame erstellen
#         df_reviews_general = pd.DataFrame(rows, columns=columns)
#         #df_reviews_general.to_csv("/home/ubuntu/OpenAI/OpenAI_test.csv", index=False)

#         print("Dataframe with reviews general was constructed")
#     except Exception as e:
#         print("Error while accessing the data", e)

### load the data
data = pd.read_csv("/home/ubuntu/OpenAI/reviews_general.csv")
data = data.head(25)

# Define the directory where you want to save the files
output_dir = "/home/ubuntu/OpenAI/"

# save the data from the csv file
csv_file_path = os.path.join(output_dir, "batches_sentiment_OpenAI.csv")

def split_dataset_into_subsets(dataframe, max_rows_per_subset=5):
    subsets = []
    num_rows = len(dataframe)
    
    for start in range(0, num_rows, max_rows_per_subset):
        end = start + max_rows_per_subset
        subset = dataframe.iloc[start:end]
        subsets.append(subset)
    
    return subsets

#####################
### Food
#####################

# df only for food_sentences and review_id
food_df = data[["review_id", "food_sentences"]]
food_df = food_df.dropna(subset=["food_sentences"]) # drop NAs

# split the dataset into subsets
food_subsets = split_dataset_into_subsets(data)

# Define the system prompt for sentiment analysis
sentiment_prompt = (
    "Rate the sentiment of the following sentences on a scale of 1 to 5, "
    "where 1 is 'very bad' and 5 is 'very good'. Return the result as a JSON object with the key 'rating'. "
    "Only include the JSON object in your response.\n\n"
    "Sentences: {sentences}\n\n"
    "JSON:"
)

# Define a function to create batch tasks for a specific category
def create_food_sentiment_batch(data):
    tasks = []
    
    for index, row in data.iterrows():
        sentences = row['food_sentences']
        review_id = row['review_id']      # Access the review_id

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

# Process only the first two subsets
for i, food in enumerate(food_subsets, start=0):  # Limit to two subsets
    # Generate tasks for the current subset
    food_tasks = create_food_sentiment_batch(food)

    # Define a unique file name for the current subset, including the path
    batch_file_name = os.path.join(output_dir, f"batch_sentiment_analysis_food_{i}.jsonl")

    # Save the tasks to the .jsonl file
    with open(batch_file_name, 'w') as file:
        for task in food_tasks:
            file.write(json.dumps(task) + '\n')

    print(f"Saved tasks for Subset {i} to {batch_file_name}")

    # # Uncomment if OpenAI batch job creation is needed
    # try:
    #     batch_file = client.files.create(
    #         file=open(batch_file_name, "rb"),
    #         purpose="batch"
    #     )
    #     print(f"Batch file uploaded: {batch_file.id}")
        
    #     # Create a batch job for the uploaded file
    #     batch_job = client.batches.create(
    #         input_file_id=batch_file.id,
    #         endpoint="/v1/chat/completions",
    #         completion_window="24h"  # This is the time window for the batch job
    #     )
    #     print(f"Batch job started: {batch_job.id}")
        
    #     # Append the information to the CSV file
    #     with open(csv_file_path, 'a', newline='') as csvfile:
    #         writer = csv.DictWriter(csvfile, fieldnames=["api_key", "batch_file_name", "batch_job_id"])
    #         writer.writerow({
    #             "api_key": client.api_key,  # Assuming `client.api_key` stores your API key
    #             "batch_file_name": batch_file_name,
    #             "batch_job_id": batch_job.id
    #         })
        
    # except Exception as e:
    #     print(f"Error uploading {batch_file_name} or creating batch job: {e}")