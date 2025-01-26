### all needed imports
from openai import OpenAI
import pandas as pd
import numpy as np
import json
client = OpenAI(
  api_key="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"
)

### load the data
data = pd.read_csv("reviews_general.csv")


#####################
### Food
#####################

# df only for food_sentences and review_id
food_df = data[["review_id", "food_sentences"]]
food_df = food_df.dropna(subset=["food_sentences"]) # drop NAs


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

# Prepare the batch tasks
food_tasks = create_food_sentiment_batch(food_df)

# Save the tasks to a .jsonl file
batch_file_name = "batch_food_sentiment.jsonl"
with open(batch_file_name, 'w') as file:
    for task in food_tasks:
        file.write(json.dumps(task) + '\n')

# Upload the file to the API
batch_file = client.files.create(
    file=open(batch_file_name, "rb"),
    purpose="batch"
)
print(f"Batch file uploaded: {batch_file.id}")

# Create a batch job
batch_job = client.batches.create(
    input_file_id=batch_file.id,
    endpoint="/v1/chat/completions",
    completion_window="24h"
)
print(f"Batch job started: {batch_job.id}")


#########
### Prepare the results to be saved

# Get the output file_id
batch_job_status = client.batches.retrieve(batch_job.id)
result_file_id = batch_job_status.output_file_id

# Download the results file
result_content = client.files.content(result_file_id).content
result_file_name = "batch_results_food.jsonl"
with open(result_file_name, 'wb') as file:
    file.write(result_content) # Save the content to a file

# Parse the results
results = []
with open(result_file_name, 'r') as file:
    for line in file:
        results.append(json.loads(line.strip()))
results = pd.DataFrame(results)

# Extract 'rating' from the 'response' column
def extract_rating(response):
    try:
        # Parse the assistant's JSON content
        content = json.loads(response['body']['choices'][0]['message']['content'])
        return content.get('rating', None)
    except Exception as e:
        print(f"Error extracting rating: {e}")
        return None

# Apply the function to extract the rating
results['food_rating'] = results['response'].apply(extract_rating)

# Keep only 'custom_id' and 'food_rating'
results = results[['custom_id', 'food_rating']]

# rename custom_id to review_id
results.rename(columns={"custom_id": "review_id"}, inplace=True)

# change data type of review_id to int64
results['review_id'] = results['review_id'].astype('int64')

# merge results on review_id to database




#####################
### Service
#####################

# df only for service_sentences and review_id
service_df = data[["review_id", "service_sentences"]]
service_df = service_df.dropna(subset=["service_sentences"]) # drop NAs


# Define the system prompt for sentiment analysis
sentiment_prompt = (
    "Rate the sentiment of the following sentences on a scale of 1 to 5, "
    "where 1 is 'very bad' and 5 is 'very good'. Return the result as a JSON object with the key 'rating'. "
    "Only include the JSON object in your response.\n\n"
    "Sentences: {sentences}\n\n"
    "JSON:"
)

# Define a function to create batch tasks for a specific category
def create_service_sentiment_batch(data):
    tasks = []
    
    for index, row in data.iterrows():
        sentences = row['service_sentences']
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

# Prepare the batch tasks
service_tasks = create_service_sentiment_batch(service_df)

# Save the tasks to a .jsonl file
batch_file_name = "batch_service_sentiment.jsonl"
with open(batch_file_name, 'w') as file:
    for task in service_tasks:
        file.write(json.dumps(task) + '\n')

# Upload the file to the API
batch_file = client.files.create(
    file=open(batch_file_name, "rb"),
    purpose="batch"
)
print(f"Batch file uploaded: {batch_file.id}")

# Create a batch job
batch_job = client.batches.create(
    input_file_id=batch_file.id,
    endpoint="/v1/chat/completions",
    completion_window="24h"
)
print(f"Batch job started: {batch_job.id}")


#########
### Prepare the results to be saved

# Get the output file_id
batch_job_status = client.batches.retrieve(batch_job.id)
result_file_id = batch_job_status.output_file_id

# Download the results file
result_content = client.files.content(result_file_id).content
result_file_name = "batch_results_service.jsonl"
with open(result_file_name, 'wb') as file:
    file.write(result_content) # Save the content to a file

# Parse the results
results = []
with open(result_file_name, 'r') as file:
    for line in file:
        results.append(json.loads(line.strip()))
results = pd.DataFrame(results)

# Extract 'rating' from the 'response' column
def extract_rating(response):
    try:
        # Parse the assistant's JSON content
        content = json.loads(response['body']['choices'][0]['message']['content'])
        return content.get('rating', None)
    except Exception as e:
        print(f"Error extracting rating: {e}")
        return None

# Apply the function to extract the rating
results['service_rating'] = results['response'].apply(extract_rating)

# Keep only 'custom_id' and 'service_rating'
results = results[['custom_id', 'service_rating']]

# rename custom_id to review_id
results.rename(columns={"custom_id": "review_id"}, inplace=True)

# change data type of review_id to int64
results['review_id'] = results['review_id'].astype('int64')

# merge results on review_id to database



#####################
### Atmosphere
#####################

# df only for atmosphere_sentences and review_id
atmosphere_df = data[["review_id", "atmosphere_sentences"]]
atmosphere_df = atmosphere_df.dropna(subset=["atmosphere_sentences"]) # drop NAs


# Define the system prompt for sentiment analysis
sentiment_prompt = (
    "Rate the sentiment of the following sentences on a scale of 1 to 5, "
    "where 1 is 'very bad' and 5 is 'very good'. Return the result as a JSON object with the key 'rating'. "
    "Only include the JSON object in your response.\n\n"
    "Sentences: {sentences}\n\n"
    "JSON:"
)

# Define a function to create batch tasks for a specific category
def create_atmosphere_sentiment_batch(data):
    tasks = []
    
    for index, row in data.iterrows():
        sentences = row['atmosphere_sentences']
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

# Prepare the batch tasks
atmosphere_tasks = create_atmosphere_sentiment_batch(atmosphere_df)

# Save the tasks to a .jsonl file
batch_file_name = "batch_atmosphere_sentiment.jsonl"
with open(batch_file_name, 'w') as file:
    for task in atmosphere_tasks:
        file.write(json.dumps(task) + '\n')

# Upload the file to the API
batch_file = client.files.create(
    file=open(batch_file_name, "rb"),
    purpose="batch"
)
print(f"Batch file uploaded: {batch_file.id}")

# Create a batch job
batch_job = client.batches.create(
    input_file_id=batch_file.id,
    endpoint="/v1/chat/completions",
    completion_window="24h"
)
print(f"Batch job started: {batch_job.id}")


#########
### Prepare the results to be saved

# Get the output file_id
batch_job_status = client.batches.retrieve(batch_job.id)
result_file_id = batch_job_status.output_file_id

# Download the results file
result_content = client.files.content(result_file_id).content
result_file_name = "batch_results_atmosphere.jsonl"
with open(result_file_name, 'wb') as file:
    file.write(result_content) # Save the content to a file

# Parse the results
results = []
with open(result_file_name, 'r') as file:
    for line in file:
        results.append(json.loads(line.strip()))
results = pd.DataFrame(results)

# Extract 'rating' from the 'response' column
def extract_rating(response):
    try:
        # Parse the assistant's JSON content
        content = json.loads(response['body']['choices'][0]['message']['content'])
        return content.get('rating', None)
    except Exception as e:
        print(f"Error extracting rating: {e}")
        return None

# Apply the function to extract the rating
results['atmosphere_rating'] = results['response'].apply(extract_rating)

# Keep only 'custom_id' and 'atmosphere_rating'
results = results[['custom_id', 'atmosphere_rating']]

# rename custom_id to review_id
results.rename(columns={"custom_id": "review_id"}, inplace=True)

# change data type of review_id to int64
results['review_id'] = results['review_id'].astype('int64')

# merge results on review_id to database