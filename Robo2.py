############################################################################################################
##### Preparations
############################################################################################################

### General preparations
import pandas as pd
import numpy as np
from openai import OpenAI
import psycopg2
import json
import time
import sys
import os

client = OpenAI(
  api_key="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" # Tier 2 key
)

### Creating connection to the database
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
    print(f"Failed to connect to database: {str(e)}")
    sys.exit(1)

### Data preparations

# Define how the data is splitted into batches
def create_review_batches(data, batch_size=45000):
    # Sort data by review_id
    data = data.sort_values(by="review_id").reset_index(drop=True)
    
    batches = []
    start_index = 0
    while start_index < len(data):
        start_review_id = data.loc[start_index, "review_id"]
        # Find the index where batch would exceed 45,000 reviews
        end_index = min(start_index + batch_size, len(data))
        # Adjust end_index to ensure we don't exceed batch_size and don't split a restaurant
        while (
            end_index < len(data) and 
            data.loc[end_index, "restaurant_id"] == data.loc[end_index - 1, "restaurant_id"]
        ):
            end_index -= 1  # Move back to avoid splitting the restaurant
        # Ensure at least one review in the batch
        if end_index <= start_index:
            end_index = start_index + 1  
        # Get the last review ID for the batch
        end_review_id = data.loc[end_index - 1, "review_id"]
        # Store batch info
        batches.append({"start_review_id": start_review_id, "end_review_id": end_review_id})
        # Move to next batch
        start_index = end_index  # Start from the next review

    return pd.DataFrame(batches)


# function to preprocess review text
def preprocess_reviews(data):
    data = data.dropna(subset=['review_text'])  # Drop rows with missing review_text
    data['review_text'] = data['review_text'].str.replace(r'\s+', ' ', regex=True).str.strip()  # Remove extra spaces, newlines, tabs
    data = data[data["review_text"].str.len() > 10]  # Keep only reviews longer than 10 characters
    return data


### Functions that will be used throughout different tasks

# Define a function to split tasks into a maximum of two batches
def split_into_batches(tasks, batch_size=45000):
    if len(tasks) <= batch_size:
        return [tasks]
    return [tasks[:batch_size], tasks[batch_size:batch_size*2]]


## Function for batch processing:
def batch_processing(batches, prefix):
    batch_file_names = []
    batch_file_ids = []
    batch_job_ids = []

    # 1. Save each batch to a separate .jsonl file
    for i, batch in enumerate(batches):
        batch_file_name = f"{prefix}_part{i+1}.jsonl"
        with open(batch_file_name, 'w') as file:
            for task in batch:
                file.write(json.dumps(task) + '\n')
        batch_file_names.append(batch_file_name)
    
    # 2. Upload batch files to the API
    for batch_file_name in batch_file_names:
        batch_file = client.files.create(
            file=open(batch_file_name, "rb"),
            purpose="batch"
        )
        print(f"Batch file uploaded: {batch_file.id}")
        batch_file_ids.append(batch_file.id)
    
    # 3. Start batch jobs
    for file_id in batch_file_ids:
        batch_job = client.batches.create(
            input_file_id=file_id,
            endpoint="/v1/chat/completions",
            completion_window="24h"
        )
        print(f"Batch job started: {batch_job.id}")
        batch_job_ids.append(batch_job.id)
    
    return batch_job_ids  # Return the list of batch job IDs


# Function to check batch job status, to wait for completion
def wait_for_batches_to_complete(batch_job_ids, batches_protocol, idx, failed_task):
    while True:
        completed_batches = 0
        failed_batches = 0
        total_batches = len(batch_job_ids)

        # Track failed batch job IDs
        failed_batch_ids = []

        for batch_job_id in batch_job_ids:
            batch_job = client.batches.retrieve(batch_job_id)
            status = batch_job.status
            print(f"Batch Job {batch_job_id} Status: {status}")

            if status == "completed":
                completed_batches += 1
            elif status in ["failed", "cancelled"]:
                failed_batches += 1
                failed_batch_ids.append(batch_job_id)  # Store failed job ID

        # If all batches are completed, proceed
        if completed_batches == total_batches:
            print("All batch jobs are completed.")
            return  # Exit the function

        # If all batches have failed
        if failed_batches == total_batches:
            print("All batch jobs have failed. Waiting for 1 hour...")
            # Store the failed job IDs in the batches_protocol for both jobs
            batches_protocol.loc[idx, failed_task] = failed_batch_ids
            time.sleep(3600)  # Wait for 1 hour before continuing to the next batch (to ensure cancellation or failed jobs are processed)
            return  # After waiting, return so the next batch can be processed

        # Only exit if all batches are either completed or failed
        if completed_batches + failed_batches == total_batches:
            print("At least one batch is failed.")
            # Update batches_protocol with the failed job IDs if any
            if failed_batches > 0:
                batches_protocol.loc[idx, failed_task] = failed_batch_ids
            return  # Exit the function

        # Wait before checking again
        print(f"Waiting 60 seconds before checking again...")
        time.sleep(60)





# Function to check batch job status, to wait for completion
def wait_for_batches_to_complete(batch_job_ids):
    while True:
        all_done = True
        for batch_job_id in batch_job_ids:
            batch_job = client.batches.retrieve(batch_job_id)
            status = batch_job.status
            print(f"Batch Job {batch_job_id} Status: {status}")

            if status not in ['completed', 'failed', 'canceled']:
                all_done = False  # Keep waiting if any batch is still running

        if all_done:
            print("All batch jobs are completed.")
            break  # Exit loop when all batches are done
        
        print(f"Waiting 60 seconds before checking again...")
        time.sleep(60)  # Wait before checking again



def insert_data_into_table(connection, cursor, table_name, dataframe, columns):
    try:
        insert_query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({', '.join(['%s'] * len(columns))});
        """
        
        rows_to_insert = dataframe[columns].values.tolist()
        
        cursor.executemany(insert_query, rows_to_insert)
        connection.commit() 
    
    except Exception as e:
        print(f"Error loading the data into '{table_name}': {str(e)}")
        connection.rollback() 


def update_database_overall_summary(summaries, idx):
    try:
        table_name = "restaurant_general"
        update_query = f"""
            UPDATE {table_name} 
            SET summary_overall = %s,
                user_count_overall = %s
            WHERE restaurant_id = %s;
        """
        
        rows_to_update = [
            (row['overall_summary'], row['user_count_overall'], row['restaurant_id'])
            for row in summaries
        ]

        cursor.executemany(update_query, rows_to_update)
        connection.commit()
        print(f"Successfully updated {len(rows_to_update)} rows.")
    except Exception as e:
        print(f"Error updating table '{table_name}': {str(e)}")
        connection.rollback()

        # Backup to save the csv files directly on the server
        backup_dir = "/mnt/volume/backup_summaries"
        os.makedirs(backup_dir, exist_ok=True)  # Erstellt das Verzeichnis, falls es nicht existiert

        try:
            summaries_df = pd.DataFrame(summaries)
            backup_path = os.path.join(backup_dir, f"overall_summaries_{idx}.csv")
            summaries_df.to_csv(backup_path, index=False, encoding="utf-8")  # Setze UTF-8 Encoding
            print(f"Backup saved at {backup_path}")

        except Exception as e:
            print(f"Failed to backup summaries on the server: {e}, {idx}")
        



def update_database_topic_summary(summaries, idx):
    try:
        table_name = "restaurant_general"
        update_query = f"""
            UPDATE {table_name} 
            SET summary_food = %s, 
                summary_service = %s, 
                summary_atmosphere = %s, 
                summary_price = %s,
                user_count_food = %s, 
                user_count_service = %s, 
                user_count_atmosphere = %s, 
                user_count_price = %s
            WHERE restaurant_id = %s;
        """
        
        rows_to_update = [
            (row['summary_food'], row['summary_service'], row['summary_atmosphere'], row['summary_price'], 
            row['user_count_food'], row['user_count_service'], row['user_count_atmosphere'], row['user_count_price'],
            row['restaurant_id'])
            for row in summaries
        ]


        cursor.executemany(update_query, rows_to_update)
        connection.commit()
        print(f"Successfully updated {len(rows_to_update)} rows.")

    except Exception as e:
        print(f"Error updating table '{table_name}': {str(e)}")
        connection.rollback()

        # Backup to save the csv files directly on the server
        backup_dir = "/mnt/volume/backup_summaries"
        os.makedirs(backup_dir, exist_ok=True)  # Erstellt das Verzeichnis, falls es nicht existiert

        try:
            summaries_df = pd.DataFrame(summaries)
            backup_path = os.path.join(backup_dir, f"topic_summaries_{idx}.csv")
            summaries_df.to_csv(backup_path, index=False, encoding="utf-8")  # Setze UTF-8 Encoding
            print(f"Backup saved at {backup_path}")

        except Exception as e:
            print(f"Failed to backup summaries on the server: {e}, {idx}")


############################################################################################################
##### 1. Topic extraction
############################################################################################################

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

def create_batch_tasks_topic_extraction(batch_1, batch_2):
    def create_tasks(data):
        tasks = []
        for index, row in data.iterrows():
            review_text = row['review_text']
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

    return create_tasks(batch_1), create_tasks(batch_2)


############################################################################################################
##### 2. + 6. Generate summaries
############################################################################################################

# Define prompts for the overall summaries
OVERALL_SUMMARY_PROMPT = (
    "You are an expert summarizer specializing in restaurant reviews. Summarize the following reviews for a restaurant. "
    "Be sure to include the overall tone of the reviews. "
    "Write concisely, strictly in English and limit the overall response to around 400 characters.\n\n"
    "Reviews:\n{reviews}\n\n"
    "Summary:"
)

OVERALL_SUMMARY_COMBINE_PROMPT = (
    "You are an expert summarizer specializing in restaurant reviews. "
    "Combine the following two summaries into a single concise and cohesive summary in English. "
    "The summary should be limited to around 200 characters:\n\n"
    "Summary 1:\n{summary1}\n\n"
    "Summary 2:\n{summary2}\n\n"
    "Combined Summary:"
)

FOOD_SUMMARY_PROMPT = (
    "You are an expert summarizer specializing in customer opinions about the food in a restaurant. "
    "The summary should focus exclusively on customer perceptions of the food, including aspects like taste, presentation, freshness, and variety. "
    "Do not include information about price, service, or atmosphere. "
    "List up to the 5 most positively recommended items in a second section as bullet points. "
    "Only include food items in the recommendations section if customers mention them positively. "
    "Do not list items with mixed or negative reviews. "
    "Write concisely, strictly in English and limit the overall response to around 400 characters.\n\n"
    "Reviews:\n{reviews}\n\n"
    "Summary:"
)

FOOD_COMBINE_PROMPT = (
    "You are an expert summarizer specializing in customer opinions about food. Combine the following two summaries into a single cohesive summary in English. "
    "The combined summary should focus exclusively on customer perceptions of the food, including aspects like taste, presentation, freshness, and variety. "
    "Do not include information about price, service, or atmosphere. "
    "List up to the 5 most positively recommended items in a second section as bullet points. "
    "Only include food items in the recommendations section if customers mention them positively. "
    "Do not list items with mixed or negative reviews. "
    "Limit the overall response to around 400 characters:\n\n"
    "Summary 1:\n{summary1}\n\n"
    "Summary 2:\n{summary2}\n\n"
    "Combined Summary:"
)

SERVICE_SUMMARY_PROMPT = (
    "You are an expert summarizer specializing in customer opinions about the service in restaurants. "
    "The summary should focus exclusively on customer perceptions of the service, including aspects like speed, attentiveness, friendliness, and professionalism. "
    "Do not include information about price, food, or atmosphere. "
    "Write concisely, strictly in English and limit the overall response to around 400 characters.\n\n"
    "Reviews:\n{reviews}\n\n"
    "Summary:"
)

SERVICE_COMBINE_PROMPT = (
    "You are an expert summarizer specializing in customer opinions about service in restaurants. Combine the following two summaries into a single cohesive summary in English."
    "The combined summary should focus exclusively on customer perceptions of the service, including aspects like speed, attentiveness, friendliness, and professionalism. "
    "Do not include information about price, food, or atmosphere. "
    "Limit the overall response to around 400 characters:\n\n"
    "Summary 1:\n{summary1}\n\n"
    "Summary 2:\n{summary2}\n\n"
    "Combined Summary:"
)

ATMOSPHERE_SUMMARY_PROMPT = (
    "You are an expert summarizer specializing in customer opinions about the atmosphere in restaurants. "
    "The summary should focus exclusively on customer perceptions of the atmosphere, including aspects like ambiance, decor, cleanliness, noise levels, and overall vibe. "
    "Do not include information about price, food, or service. "
    "Write concisely, strictly in English and limit the overall response to around 400 characters.\n\n"
    "Reviews:\n{reviews}\n\n"
    "Summary:"
)

ATMOSPHERE_COMBINE_PROMPT = (
    "You are an expert summarizer specializing in customer opinions about the atmosphere in restaurants. Combine the following two summaries into a single cohesive summary in English. "
    "The combined summary should focus exclusively on customer perceptions of the atmosphere, including aspects like ambiance, decor, cleanliness, noise levels, and overall vibe. "
    "Do not include information about price, food, or service. "
    "Limit the overall response to around 400 characters:\n\n"
    "Summary 1:\n{summary1}\n\n"
    "Summary 2:\n{summary2}\n\n"
    "Combined Summary:"
)

PRICE_SUMMARY_PROMPT = (
    "You are an expert summarizer specializing in customer opinions about pricing in restaurants. "
    "The summary should focus exclusively on customer perceptions of the price, including aspects like value for money, affordability, and pricing fairness. "
    "Do not include information about food, service, or atmosphere. "
    "Write concisely, strictly in English and limit the overall response to around 400 characters.\n\n"
    "Reviews:\n{reviews}\n\n"
    "Summary:"
)

PRICE_COMBINE_PROMPT = (
    "You are an expert summarizer specializing in customer opinions about pricing in restaurants. Combine the following two summaries into a single cohesive summary in English. "
    "The combined summary should focus exclusively on customer perceptions of the price, including aspects like value for money, affordability, and pricing fairness. "
    "Do not include information about food, service, or atmosphere. "
    "Limit the overall response to around 400 characters:\n\n"
    "Summary 1:\n{summary1}\n\n"
    "Summary 2:\n{summary2}\n\n"
    "Combined Summary:"
)

# Function to summarize a single chunk of reviews using modular prompts
def summarize_chunk(prompt, reviews_chunk):
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt.format(reviews=reviews_chunk)},
            ],
            max_tokens=200,
            temperature=0.7,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"Error summarizing chunk: {e}")
        return None
    

# Function to combine summaries using modular prompts
def combine_summaries(combine_prompt, summary1, summary2):
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": combine_prompt.format(summary1=summary1, summary2=summary2)},
            ],
            max_tokens=200,
            temperature=0.7,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"Error combining summaries: {e}")
        return f"{summary1}\n\n{summary2}"

    

# Function to handle larger price reviews, retry, and combine summaries
def summarize_reviews(restaurant_id, reviews_df, category_column_name, summary_prompt, combine_prompt):
    """
    Generalized function to summarize reviews for a specific aspect (overall, service, atmosphere, etc.).
    
    Args:
        restaurant_id (int): ID of the restaurant.
        reviews_df (DataFrame): DataFrame containing review data.
        category_column_name (str): Column in the DataFrame containing the reviews for this aspect.
        summary_prompt (str): Prompt for summarizing reviews.
        combine_prompt (str): Prompt for combining summaries.
    
    Returns:
        str: Final summarized review.
    """
    # Filter and join the reviews for the specified column
    reviews = reviews_df[reviews_df['restaurant_id'] == restaurant_id][category_column_name].str.strip().replace(["", "nan", "None", "null"], pd.NA).dropna().astype(str).tolist()
    
    # If no reviews exist, return None
    if not reviews:
        return None, 0  # No summary, zero review count
    
    # Join the reviews into a single text chunk
    reviews_text = "\n".join(reviews)

    # Getting amount of reviews
    count_reviews = len(reviews)
    
    try:
        # Attempt to summarize the full chunk
        return summarize_chunk(summary_prompt, reviews_text), count_reviews
    
    except Exception as e:
        if "context_length_exceeded" in str(e):
            print(f"Context length exceeded for restaurant '{restaurant_id}' in column '{category_column_name}'. Splitting reviews...")
            
            # Split reviews into halves
            mid_point = len(reviews) // 2
            first_half = "\n".join(reviews[:mid_point])
            second_half = "\n".join(reviews[mid_point:])
            
            # Summarize each half
            first_summary = summarize_chunk(summary_prompt, first_half)
            second_summary = summarize_chunk(summary_prompt, second_half)
            
            if first_summary and second_summary:
                # Combine summaries
                try:
                    return combine_summaries(combine_prompt, first_summary, second_summary), count_reviews
                except Exception as combine_error:
                    print(f"Error combining summaries for restaurant '{restaurant_id}' in column '{category_column_name}': {combine_error}")
                    return f"{first_summary}", (len(reviews) // 2)  # Return the first summary if combining fails
            else:
                return f"{first_summary or 'Error in first half'}\n\n{second_summary or 'Error in second half'}", (len(reviews) // 2)
        else:
            print(f"Error summarizing reviews for restaurant '{restaurant_id}' in column '{category_column_name}': {e}")
            return None, None



############################################################################################################
##### 4. Retrieve categorized sentences
############################################################################################################

# Function to retrieve batch results for categorized sentences
def retrieve_batch_results_categorized(batch_job_ids):
    all_results = []

    for batch_job_id in batch_job_ids:
        # Get the output file ID
        batch_job_status = client.batches.retrieve(batch_job_id)
        result_file_id = batch_job_status.output_file_id

        # ! think about error handeling !
        #if not result_file_id:
        #    print(f" No output file found for batch job {batch_job_id}. Skipping...")
        #    continue

        # Download the results file
        result_content = client.files.content(result_file_id).content
        result_file_name = f"batch_results_{batch_job_id}.jsonl"
        with open(result_file_name, 'wb') as file:
            file.write(result_content)  # Save the content to a file

        # Parse the results
        with open(result_file_name, 'r') as file:
            for line in file:
                all_results.append(json.loads(line.strip()))

    # Convert results to DataFrame
    results_df = pd.DataFrame(all_results)
    return results_df


# Function to extract sentences for each topic
def extract_categorized_sentences(response):
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


############################################################################################################
##### 5. Sentiment analysis
############################################################################################################
# Define the system prompt for sentiment analysis
sentiment_prompt = (
    "Rate the sentiment of the following sentences on a scale of 1 to 5, "
    "where 1 is 'very bad' and 5 is 'very good'. Return the result as a JSON object with the key 'rating'. "
    "Only include the JSON object in your response.\n\n"
    "Sentences: {sentences}\n\n"
    "JSON:"
)

# Define a function to create batch tasks for any category
def create_sentiment_batch(data, category):
    tasks = []
    category_column = f"{category}_sentences"

    for index, row in data.iterrows():
        sentences = row[category_column]
        review_id = row['review_id']  # Access the review_id

        if not sentences or pd.isna(sentences):  # Skip empty or NaN sentences
            continue

        task = {
            "custom_id": f"{review_id}_{category}",
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


############################################################################################################
##### 8. Retrieve sentiment results (subratings)
############################################################################################################
def retrieve_batch_results_subratings(batch_job_ids, category):
    results = []

    for batch_job_id in batch_job_ids:
        # Get batch job status
        batch_job_status = client.batches.retrieve(batch_job_id)
        result_file_id = batch_job_status.output_file_id

        # ! think about error handeling !
        if not result_file_id:
            print(f"No output file found for batch job {batch_job_id}")
            continue  # Skip if there's no result file

        # Download the batch results file
        result_content = client.files.content(result_file_id).content
        result_file_name = f"batch_results_{category}.jsonl"

        with open(result_file_name, 'wb') as file:
            file.write(result_content)

        # Read and parse the results
        with open(result_file_name, 'r') as file:
            for line in file:
                try:
                    response = json.loads(line.strip())
                    custom_id = response['custom_id']  # Format: review_id_category
                    review_id, _ = custom_id.rsplit("_", 1)  # Extract review_id
                    
                    # Extract rating from response
                    body = response.get("response", {}).get("body", {})
                    choices = body.get("choices", [])
                    if choices:
                        content = json.loads(choices[0]["message"]["content"])
                        rating = content.get('rating', None)
                    else:
                        rating = None

                    # Append results
                    results.append({"review_id": int(review_id), f"rating_{category}": rating})

                except Exception as e:
                    print(f"Error parsing result for {category}: {e}")

    return pd.DataFrame(results)




############################################################################################################
##### The overall process
############################################################################################################

### Create a protocol to keep track of the batches
# Prepare how the data is splitted into batches
# Getting relevant data from the database
try:
    query_general = """
        SELECT review_id, restaurant_id
        FROM reviews_general
        WHERE review_id >= 1348159;
        """

    cursor.execute(query_general)
    rows = cursor.fetchall()

    columns = [desc[0] for desc in cursor.description]

    data = pd.DataFrame(rows, columns=columns)

except Exception as e:
    print(f"Failed to retrieve general information from the database {e}")
    sys.exit(1)     # exiting the file if data can't be retrieved


batches_protocol = create_review_batches(data, batch_size=45000)

# Initialize new columns in batches_protocol DataFrame for job IDs
batches_protocol['topic_job_ids'] = None


# Iterate over batches_protocol
for idx in range(0, len(batches_protocol), 2):  # Process two batches at a time
    # Extract the batch data using the batch protocol
    min_review_id_1 = batches_protocol.loc[idx, "start_review_id"]
    max_review_id_1 = batches_protocol.loc[idx, "end_review_id"]

    try:
        query_general = f"""
            SELECT review_id, restaurant_id, review_text
            FROM reviews_general
            WHERE review_id between {min_review_id_1} AND {max_review_id_1};
            """

        cursor.execute(query_general)
        rows = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]

        data_1 = pd.DataFrame(rows, columns=columns)

    except Exception as e:
        print(f"Failed to retrieve data for batch 1 from the database, index {idx} {e}")
        sys.exit(1)     # exiting the file if data can't be retrieved



    # Data for batch 2
    if idx + 1 < len(batches_protocol):
        min_review_id_2 = batches_protocol.loc[idx + 1, "start_review_id"]
        max_review_id_2 = batches_protocol.loc[idx + 1, "end_review_id"]

        try:
            query_general = f"""
                SELECT review_id, restaurant_id, review_text
                FROM reviews_general
                WHERE review_id between {min_review_id_2} AND {max_review_id_2};
                """

            cursor.execute(query_general)
            rows = cursor.fetchall()

            columns = [desc[0] for desc in cursor.description]

            data_2 = pd.DataFrame(rows, columns=columns)

        except Exception as e:
            print(f"Failed to retrieve data for batch 2 from the database, index {idx} {e}")
            sys.exit(1)     # exiting the file if data can't be retrieved
    else:
        data_2 = pd.DataFrame()  # In case there is no second batch, use an empty DataFrame
    
    # Apply preprocessing for both batches
    data_1 = preprocess_reviews(data_1)
    if not data_2.empty:
        data_2 = preprocess_reviews(data_2)
    

    #### 1. Topic extraction
    # Create batch tasks for both batches
    tasks_topic_1, tasks_topic_2 = create_batch_tasks_topic_extraction(data_1, data_2)
    # Save, Upload, and Start batch jobs
    topic_job_ids = batch_processing(batches=[tasks_topic_1, tasks_topic_2], prefix=f"topic_extraction")
    
    # Store topic job IDs in the batches_protocol DataFrame
    batches_protocol.loc[idx, 'topic_job_ids'] = topic_job_ids[0]  # First batch
    if idx + 1 < len(batches_protocol):
        batches_protocol.loc[idx + 1, 'topic_job_ids'] = topic_job_ids[1]  # Second batch


    #### 2. Overall summaries
    # Combine both batches into one dataset
    combined_data = pd.concat([data_1, data_2])
    # Extract unique restaurant IDs of the two batches
    unique_restaurant_ids = combined_data['restaurant_id'].dropna().unique()

    overall_summaries = []  # List to store summaries
    # Generate summaries for each restaurant
    for restaurant_id in unique_restaurant_ids:
        overall_summary, user_count_overall = summarize_reviews(
            restaurant_id, combined_data, 'review_text', 
            OVERALL_SUMMARY_PROMPT, OVERALL_SUMMARY_COMBINE_PROMPT
        )
        overall_summaries.append({
            "restaurant_id": restaurant_id,
            "overall_summary": overall_summary,
            "user_count_overall": user_count_overall
        })
    
    # overall_summaries = pd.DataFrame(overall_summaries)  -> we don't need a dataframe

    # Saving the overall summaries into restaurant general table
    update_database_overall_summary(overall_summaries, idx)
    

    #### 3. Transition wait until topic extraction is done
    wait_for_batches_to_complete(topic_job_ids, batches_protocol, idx, failed_task="failed_topic_job_ids")


    #### 4. Retrieve categorized sentences
    try:
        results_df = retrieve_batch_results_categorized(topic_job_ids)
        category_data = results_df['response'].apply(extract_categorized_sentences)
        category_df = pd.DataFrame(category_data.tolist())
        category_df['review_id'] = results_df['custom_id'].astype('int64')
    except Exception as e:
        print(f"Error retrieving categorized sentences for batch {idx}: {e}")
        continue  # Skip to the next batch iteration


    #### 5. Sentiment analysis
    # 5.1 Food Sentiment
    food_tasks = create_sentiment_batch(category_df, "food")
    food_batches = split_into_batches(food_tasks, batch_size=45000)
    food_job_ids = batch_processing(food_batches, prefix=f"food_sentiment")

    # 5.2 Service Sentiment
    service_tasks = create_sentiment_batch(category_df, "service")
    service_batches = split_into_batches(service_tasks, batch_size=45000)
    service_job_ids = batch_processing(service_batches, prefix=f"service_sentiment")

    # 5.3 Atmosphere Sentiment
    atmosphere_tasks = create_sentiment_batch(category_df, "atmosphere")
    atmosphere_batches = split_into_batches(atmosphere_tasks, batch_size=45000)
    atmosphere_job_ids = batch_processing(atmosphere_batches, prefix=f"atmosphere_sentiment")
    
    # Collect all sentiment batch job IDs into one list
    batch_job_ids_sentiment = food_job_ids + service_job_ids + atmosphere_job_ids


    # Saving the collected sentences
    try:
        table = "reviews_subcategories"
        columns = ["review_id", "food_sentences", "service_sentences", "atmosphere_sentences", "price_sentences"]
        insert_data_into_table(connection, cursor, table, category_df, columns)

    except Exception as e:
        print(f"Failed to save to extracted topic sentences into the database{e}")



    #### 6. Category Summaries
    reviews_df = pd.merge(category_df, combined_data, on='review_id', how='left')
    reviews_df = reviews_df[['review_id', 'restaurant_id', 'food_sentences', 'service_sentences', 'atmosphere_sentences', 'price_sentences']]

    summaries_categories = []  # List to store summaries
    for restaurant_id in unique_restaurant_ids:
        food_summary, user_count_food = summarize_reviews(restaurant_id, reviews_df, 'food_sentences', FOOD_SUMMARY_PROMPT, FOOD_COMBINE_PROMPT)
        service_summary, user_count_service = summarize_reviews(restaurant_id, reviews_df, 'service_sentences', SERVICE_SUMMARY_PROMPT, SERVICE_COMBINE_PROMPT)
        atmosphere_summary, user_count_atmosphere = summarize_reviews(restaurant_id, reviews_df, 'atmosphere_sentences', ATMOSPHERE_SUMMARY_PROMPT, ATMOSPHERE_COMBINE_PROMPT)
        price_summary, user_count_price = summarize_reviews(restaurant_id, reviews_df, 'price_sentences', PRICE_SUMMARY_PROMPT, PRICE_COMBINE_PROMPT)
        
        summaries_categories.append({
            "restaurant_id": restaurant_id,
            "summary_food": food_summary,
            "summary_service": service_summary,
            "summary_atmosphere": atmosphere_summary,
            "summary_price": price_summary,
            "user_count_food": user_count_food,
            "user_count_service": user_count_service,
            "user_count_atmosphere": user_count_atmosphere,
            "user_count_price": user_count_price,
        })

    # summaries_categories = pd.DataFrame(summaries_categories)   -> we don't need a dataframe

    # Saving the topic summaries into restaurant general table
    update_database_topic_summary(summaries_categories, idx)
    



    #### 7. Transition wait until sentiment analysis is done
    wait_for_batches_to_complete(batch_job_ids_sentiment, batches_protocol, idx, failed_task="failed_sentiment_job_ids")

    #### 8. Retrieve sentiment results (subratings)
    try:
        df_food = retrieve_batch_results_subratings(food_job_ids, "food")
        df_service = retrieve_batch_results_subratings(service_job_ids, "service")
        df_atmosphere = retrieve_batch_results_subratings(atmosphere_job_ids, "atmosphere")
        
    except Exception as e:
        print(f"Error retrieving sentiment results for batch {idx}: {e}")
        continue  # Skip to the next batch iteration

    df_ratings = df_food.merge(df_service, on="review_id", how="outer").merge(df_atmosphere, on="review_id", how="outer")


    # Saving the collected subratings
    try:
        table = "reviews_subcategories"
        columns = ["review_id", "rating_food", "rating_service", "rating_atmosphere"]
        insert_data_into_table(connection, cursor, table, df_ratings, columns)

    except Exception as e:
        print(f"Failed to save to extracted topic sentences into the database{e}")


