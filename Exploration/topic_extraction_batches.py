### all needed imports
from openai import OpenAI
import pandas as pd
import numpy as np
import json
import os

# API key for OpenAI
openai_api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(
  api_key=openai_api_key
)

### load the data and preprocess it
data = pd.read_csv("reviews_general_selected.csv")
data = data.dropna(subset=['review_text'])
# remove extra spaces, newlines, and tabs
data['review_text'] = data['review_text'].str.replace(r'\s+', ' ', regex=True).str.strip()


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

        review_text = row['review_text']
        review_id = row['review_id']  # Access the review_id from the DataFrame
        
        task = {
            "custom_id": f"{review_id}",  # Use review_id in the custom_id
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

# Prepare the batch tasks
tasks = create_batch_tasks(data)

# Save the tasks to a .jsonl file
batch_file_name = "batch_tasks_reviews.jsonl"
with open(batch_file_name, 'w') as file:
    for task in tasks:
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


####################################
### Prepare the results to be saved
####################################

# Get the output file_id
batch_job_status = client.batches.retrieve(batch_job.id)
result_file_id = batch_job_status.output_file_id

# Download the results file
result_content = client.files.content(result_file_id).content
result_file_name = "batch_results.jsonl"
with open(result_file_name, 'wb') as file:
    file.write(result_content) # Save the content to a file

# Parse the results
results = []
with open(result_file_name, 'r') as file:
    for line in file:
        results.append(json.loads(line.strip()))
results = pd.DataFrame(results)

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

# Apply the extraction to the 'response' column
category_data = results['response'].apply(extract_sentences)

# Create a new DataFrame with the extracted category sentences
category_df = pd.DataFrame(category_data.tolist())

# Combine 'custom_id' and extracted category sentences
category_df['custom_id'] = results['custom_id']
# rename custom_id to review_id
category_df.rename(columns={"custom_id": "review_id"}, inplace=True)


# merge category_df on review_id and add to database