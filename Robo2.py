############################################################################################################
##### Preparations
############################################################################################################

### General preparations
import pandas as pd
import numpy as np
from openai import OpenAI
import json
import time

client = OpenAI(
  api_key="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" # Tier 2 key
)

### Data preparations
data = pd.read_csv("reviews_general.csv") # as an example

# preprocess the data
data = data.dropna(subset=['review_text']) # drop rows with missing review_text
data['review_text'] = data['review_text'].str.replace(r'\s+', ' ', regex=True).str.strip() # remove extra spaces, newlines, and tabs
data = data[data["review_text"].str.len() > 10] # drop rows where the "review_text" column contains less than 10 characters


### Functions that will be used throughout different tasks

# Define a function to split tasks into a maximum of two batches
def split_into_batches(tasks, batch_size=45000):
    if len(tasks) <= batch_size:
        return [tasks]
    return [tasks[:batch_size], tasks[batch_size:batch_size*2]]


## Functions for batch processing:
# 1. Function to save each batch to a separate .jsonl file
def save_batches_to_jsonl(batches, prefix):
    batch_file_names = []
    
    for i, batch in enumerate(batches):
        batch_file_name = f"{prefix}_part{i+1}.jsonl"
        with open(batch_file_name, 'w') as file:
            for task in batch:
                file.write(json.dumps(task) + '\n')
        batch_file_names.append(batch_file_name)
    
    return batch_file_names  # Return the list of saved file names

# 2. Function to upload batch files to the API
def upload_batch_files(batch_file_names):
    batch_file_ids = []
    
    for batch_file_name in batch_file_names:
        batch_file = client.files.create(
            file=open(batch_file_name, "rb"),
            purpose="batch"
        )
        print(f"Batch file uploaded: {batch_file.id}")
        batch_file_ids.append(batch_file.id)
    
    return batch_file_ids  # Return the list of uploaded file IDs

# 3. Function to start batch jobs
def start_batch_jobs(batch_file_ids):
    batch_job_ids = []
    
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
def wait_for_batches_to_complete(batch_job_ids):
    while True:
        all_done = True
        for batch_job_id in batch_job_ids:
            batch_job = client.batches.retrieve(batch_job_id)
            status = batch_job.status
            print(f"Batch Job {batch_job_id} Status: {status}")

            if status != 'completed':
                all_done = False  # Keep waiting if any batch is still running

        if all_done:
            print("All batch jobs are completed.")
            break  # Exit loop when all batches are done
        
        print(f"Waiting 60 seconds before checking again...")
        time.sleep(60)  # Wait before checking again


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

# Define a function to create tasks for batch processing
def create_batch_tasks_topic_extraction(data):
    tasks = []
    
    for index, row in data.iterrows():
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

#### 1. Topic extraction
# Prepare the batch tasks as a list
tasks_topic_extraction = create_batch_tasks_topic_extraction(data)
# Split tasks into batches of 4500 each
batches_topic_extraction = split_into_batches(tasks_topic_extraction, batch_size=45000)
# Create, upload, and start batch jobs
batch_file_names_topic_extraction = save_batches_to_jsonl(batches_topic_extraction, prefix="batch_tasks_topic_extraction")
batch_file_ids_topic_extraction = upload_batch_files(batch_file_names_topic_extraction)
batch_job_ids_topic_extraction = start_batch_jobs(batch_file_ids_topic_extraction)


#### 2. Overall summaries
# Extract unique restaurant IDs of the two batches
unique_restaurant_ids = data['restaurant_id'].dropna().unique()

overall_summaries = []  # List to store summaries
# Generate summaries for each restaurant
for restaurant_id in unique_restaurant_ids:
    # Summarize overall reviews
    overall_summary, user_count_overall = summarize_reviews(
        restaurant_id, data, 'review_text', 
        OVERALL_SUMMARY_PROMPT, OVERALL_SUMMARY_COMBINE_PROMPT
    )
    # Append the summaries to the list
    overall_summaries.append({
        "restaurant_id": restaurant_id,
        "overall_summary": overall_summary,
        "user_count_overall": user_count_overall
    })
# Convert the list of summaries into a DataFrame
overall_summaries = pd.DataFrame(overall_summaries)


#### 3. Transition wait until 1. Topic extraction is done
wait_for_batches_to_complete(batch_job_ids_topic_extraction)


#### 4. Retrieve categorized sentences
# Retrieve and process results
results_df = retrieve_batch_results_categorized(batch_job_ids_topic_extraction)
# Apply the extraction to the 'response' column
category_data = results_df['response'].apply(extract_categorized_sentences)
# Create a new DataFrame with the extracted category sentences
category_df = pd.DataFrame(category_data.tolist())
# Add 'custom_id' as 'review_id'
category_df['review_id'] = results_df['custom_id'].astype('int64')


#### 5. Sentiment analysis
## 5.1 Food Sentiment
# Prepare the batch tasks as a list
food_tasks = create_sentiment_batch(category_df, "food")
# Split tasks into batches of 45000 each
batches_food = split_into_batches(food_tasks, batch_size=45000)
# Create, upload, and start batch jobs
batch_file_names_food = save_batches_to_jsonl(batches_food, prefix="batch_tasks_food")
batch_file_ids_food = upload_batch_files(batch_file_names_food)
batch_job_ids_food = start_batch_jobs(batch_file_ids_food)

## 5.2 Service Sentiment
# Prepare the batch tasks as a list
service_tasks = create_sentiment_batch(category_df, "service")
# Split tasks into batches of 45000 each
batches_service = split_into_batches(service_tasks, batch_size=45000)
# Create, upload, and start batch jobs
batch_file_names_service = save_batches_to_jsonl(batches_service, prefix="batch_tasks_service")
batch_file_ids_service = upload_batch_files(batch_file_names_service)
batch_job_ids_service = start_batch_jobs(batch_file_ids_service)

## 5.3 Atmosphere Sentiment
# Prepare the batch tasks as a list
atmosphere_tasks = create_sentiment_batch(category_df, "atmosphere")
# Split tasks into batches of 45000 each
batches_atmosphere = split_into_batches(atmosphere_tasks, batch_size=45000)
# Create, upload, and start batch jobs
batch_file_names_atmosphere = save_batches_to_jsonl(batches_atmosphere, prefix="batch_tasks_atmosphere")
batch_file_ids_atmosphere = upload_batch_files(batch_file_names_atmosphere)
batch_job_ids_atmosphere = start_batch_jobs(batch_file_ids_atmosphere)

# Collect all batch job IDs into one list
batch_job_ids_sentiment = batch_job_ids_food + batch_job_ids_service + batch_job_ids_atmosphere


#### 6. Category Summaries
## Prepare data such that categorized sentences have restaurant_ids
# merge the categorized sentences with reviews general to get the restaurant_ids
reviews_df = pd.merge(category_df, data, on='review_id', how='left')
# keep only the necessary columns
reviews_df = reviews_df[['review_id', 'restaurant_id', 'food_sentences', 'service_sentences', 'atmosphere_sentences', 'price_sentences']]

# Extract unique restaurant IDs of the two batches
unique_restaurant_ids = reviews_df['restaurant_id'].dropna().unique()

summaries_categories = []  # List to store summaries
# Generate summaries for each restaurant
for restaurant_id in unique_restaurant_ids:
    # Summarize reviews for each category
    food_summary, user_count_food = summarize_reviews(restaurant_id, reviews_df, 'food_sentences', FOOD_SUMMARY_PROMPT, FOOD_COMBINE_PROMPT)
    service_summary, user_count_service = summarize_reviews(restaurant_id, reviews_df, 'service_sentences', SERVICE_SUMMARY_PROMPT, SERVICE_COMBINE_PROMPT)
    atmosphere_summary, user_count_atmosphere = summarize_reviews(restaurant_id, reviews_df, 'atmosphere_sentences', ATMOSPHERE_SUMMARY_PROMPT, ATMOSPHERE_COMBINE_PROMPT)
    price_summary, user_count_price = summarize_reviews(restaurant_id, reviews_df, 'price_sentences', PRICE_SUMMARY_PROMPT, PRICE_COMBINE_PROMPT)
    
    # Append the summaries to the list
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

# Convert the list of summaries into a DataFrame
summaries_categories = pd.DataFrame(summaries_categories)


#### 7. Transition wait until 5. Sentiment analysis is done
wait_for_batches_to_complete(batch_job_ids_sentiment)


#### 8. Retrieve sentiment results (subratings)
# Retrieve results for each category
df_food = retrieve_batch_results_subratings(batch_job_ids_food, "food")
df_service = retrieve_batch_results_subratings(batch_job_ids_service, "service")
df_atmosphere = retrieve_batch_results_subratings(batch_job_ids_atmosphere, "atmosphere")

# Merge all results into a single DataFrame
df_ratings = df_food.merge(df_service, on="review_id", how="outer").merge(df_atmosphere, on="review_id", how="outer")

