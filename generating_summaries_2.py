import pandas as pd
import numpy as np
from openai import OpenAI
import psycopg2
import json
import os

# API key for OpenAI
openai_api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(
  api_key=openai_api_key
)

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







### Define all prompts
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
        return None, None  # No summary, zero review count
    
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



def update_database(summaries):
    table_name = "restaurant_general"
    update_query = f"""
        UPDATE {table_name} 
        SET summary_overall = %s, 
            summary_food = %s, 
            summary_service = %s, 
            summary_atmosphere = %s, 
            summary_price = %s,
            user_count_overall = %s, 
            user_count_food = %s, 
            user_count_service = %s, 
            user_count_atmosphere = %s, 
            user_count_price = %s
        WHERE restaurant_id = %s;
    """
    
    rows_to_update = [
        (row['summary_overall'], row['summary_food'], row['summary_service'], row['summary_atmosphere'], row['summary_price'], 
        row['user_count_overall'], row['user_count_food'], row['user_count_service'], row['user_count_atmosphere'], row['user_count_price'],
        row['restaurant_id'])
        for row in summaries
    ]

    try:
        cursor.executemany(update_query, rows_to_update)
        connection.commit()
        print(f"Successfully updated {len(rows_to_update)} rows.")
    except Exception as e:
        print(f"Error updating table '{table_name}': {str(e)}")
        connection.rollback()


### load & prepare the data
# restaurant basics to get the restaurant_ids
try:
    query_general = """
        SELECT DISTINCT restaurant_id
        FROM restaurant_basics
        WHERE city_id IN (0, 1, 2);
        """

    cursor.execute(query_general)
    rows = cursor.fetchall()

    # Check, ob überhaupt Daten zurückgekommen sind
    if rows:
        columns = [desc[0] for desc in cursor.description]
        restaurant_basics = pd.DataFrame(rows, columns=columns)
        print(f"Got {len(restaurant_basics)} restaurant IDs that are NOT in city_id 0, 1, or 2.")
    else:
        restaurant_basics = pd.DataFrame()
        print("No matching restaurant IDs found.")

except Exception as e:
    print(f"Failed to get the data: {e}")









### Generate summaries for each restaurant and category

# Initialize an empty list and empty dataframe to hold the summaries
summaries = []
summaries_df = pd.DataFrame()

# Generate summaries for each restaurant and category
for index, row in restaurant_basics.iterrows():
    restaurant_id = row['restaurant_id']

    print(f"Getting data for: {restaurant_id}")

    try:
        # getting data from the database for each restaurant
        query_reviews = f"""
            SELECT r.restaurant_id, r.review_id, r.food_sentences, r.service_sentences, r.atmosphere_sentences, r.price_sentences,
                   rg.review_text
            FROM reviews_subcategories r
            LEFT JOIN reviews_general rg ON r.review_id = rg.review_id
            WHERE r.restaurant_id = '{restaurant_id}';
        """
        
        cursor.execute(query_reviews)
        rows = cursor.fetchall()

        # Spaltennamen abrufen
        columns = [desc[0] for desc in cursor.description]


        reviews_df = pd.DataFrame(rows, columns=columns)
        print(f"Got data for restaurant {restaurant_id}")

    except Exception as e:
        print(f"Failed to get data for restaurant_id {restaurant_id}: {e}")

    # preprocess reviews_df
    reviews_df = reviews_df.dropna(subset=['review_text'])
    # remove extra spaces, newlines, and tabs
    reviews_df['review_text'] = reviews_df['review_text'].str.replace(r'\s+', ' ', regex=True).str.strip()


    if not reviews_df.empty:
        print(f"Processing restaurant ID: {restaurant_id}")
        
        # Summarize overall reviews
        overall_summary, user_count_overall = summarize_reviews(restaurant_id, reviews_df, 'review_text', OVERALL_SUMMARY_PROMPT, OVERALL_SUMMARY_COMBINE_PROMPT)
        
        # Summarize food reviews
        food_summary, user_count_food = summarize_reviews(restaurant_id, reviews_df, 'food_sentences', FOOD_SUMMARY_PROMPT, FOOD_COMBINE_PROMPT)
        
        # Summarize service reviews
        service_summary, user_count_service = summarize_reviews(restaurant_id, reviews_df, 'service_sentences', SERVICE_SUMMARY_PROMPT, SERVICE_COMBINE_PROMPT)
        
        # Summarize atmosphere reviews
        atmosphere_summary, user_count_atmosphere = summarize_reviews(restaurant_id, reviews_df, 'atmosphere_sentences', ATMOSPHERE_SUMMARY_PROMPT, ATMOSPHERE_COMBINE_PROMPT)
        
        # Summarize price reviews
        price_summary, user_count_price = summarize_reviews(restaurant_id, reviews_df, 'price_sentences', PRICE_SUMMARY_PROMPT, PRICE_COMBINE_PROMPT)
        
        # Append the summaries to the list
        summaries.append({
            "restaurant_id": restaurant_id,
            "summary_overall": overall_summary,
            "summary_food": food_summary,
            "summary_service": service_summary,
            "summary_atmosphere": atmosphere_summary,
            "summary_price": price_summary,
            "user_count_overall": user_count_overall,
            "user_count_food": user_count_food,
            "user_count_service": user_count_service,
            "user_count_atmosphere": user_count_atmosphere,
            "user_count_price": user_count_price,
        })

        if (index + 1) % 10 == 0:
            print(f"Updating database for batch {index + 1}...")
            update_database(summaries)
            summaries.clear()

# Final update for any remaining summaries
if summaries:
    print("Updating database for final batch...")
    update_database(summaries)

