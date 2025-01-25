### all needed imports
from openai import OpenAI
import pandas as pd
import numpy as np
import json
client = OpenAI(
  api_key="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"
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
        
        task = {
            "custom_id": f"task-{index}",
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