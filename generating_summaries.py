import pandas as pd
import numpy as np
from openai import OpenAI

client = OpenAI(
  api_key="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" # Tier 2 key
)


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
    reviews = reviews_df[reviews_df['restaurant_id'] == restaurant_id][category_column_name].dropna().astype(str).tolist()
    reviews_text = "\n".join(reviews)
    
    try:
        # Attempt to summarize the full chunk
        return summarize_chunk(summary_prompt, reviews_text)
    
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
                    return combine_summaries(combine_prompt, first_summary, second_summary)
                except Exception as combine_error:
                    print(f"Error combining summaries for restaurant '{restaurant_id}' in column '{category_column_name}': {combine_error}")
                    return f"{first_summary}"  # Return the first summary if combining fails
            else:
                return f"{first_summary or 'Error in first half'}\n\n{second_summary or 'Error in second half'}"
        else:
            print(f"Error summarizing reviews for restaurant '{restaurant_id}' in column '{category_column_name}': {e}")
            return None


### load & prepare the data
restaurant_basics = # load the data
reviews_general = # load the data
reviews_subcategories = # load the data

# merge the data such that we have the full review text with the categorized review text
reviews_df = pd.merge(reviews_general, reviews_subcategories, on='review_id', how='left')
# keep only the necessary columns
reviews_df = reviews_df[['restaurant_id_x', 'review_id', 'review_text', 'food_sentences', 'service_sentences', 'atmosphere_sentences', 'price_sentences']]
# rename column to 'restaurant_id'
reviews_df = reviews_df.rename(columns={'restaurant_id_x': 'restaurant_id'})
# preprocess reviews_df
reviews_df = reviews_df.dropna(subset=['review_text'])
# remove extra spaces, newlines, and tabs
reviews_df['review_text'] = reviews_df['review_text'].str.replace(r'\s+', ' ', regex=True).str.strip()


### Generate summaries for each restaurant and category

# Initialize an empty list to hold the summaries
summaries = []

# Generate summaries for each restaurant and category
for _, row in restaurant_basics.iterrows():
    restaurant_id = row['restaurant_id']
    
    print(f"Processing restaurant ID: {restaurant_id}")
    
    # Summarize overall reviews
    overall_summary = summarize_reviews(restaurant_id, reviews_df, 'review_text', OVERALL_SUMMARY_PROMPT, OVERALL_SUMMARY_COMBINE_PROMPT)
    
    # Summarize food reviews
    food_summary = summarize_reviews(restaurant_id, reviews_df, 'food_sentences', FOOD_SUMMARY_PROMPT, FOOD_COMBINE_PROMPT)
    
    # Summarize service reviews
    service_summary = summarize_reviews(restaurant_id, reviews_df, 'service_sentences', SERVICE_SUMMARY_PROMPT, SERVICE_COMBINE_PROMPT)
    
    # Summarize atmosphere reviews
    atmosphere_summary = summarize_reviews(restaurant_id, reviews_df, 'atmosphere_sentences', ATMOSPHERE_SUMMARY_PROMPT, ATMOSPHERE_COMBINE_PROMPT)
    
    # Summarize price reviews
    price_summary = summarize_reviews(restaurant_id, reviews_df, 'price_sentences', PRICE_SUMMARY_PROMPT, PRICE_COMBINE_PROMPT)
    
    # Append the summaries to the list
    summaries.append({
        "restaurant_id": restaurant_id,
        "overall_summary": overall_summary,
        "food_summary": food_summary,
        "service_summary": service_summary,
        "atmosphere_summary": atmosphere_summary,
        "price_summary": price_summary,
    })

# Convert the list of summaries into a DataFrame
summaries_df = pd.DataFrame(summaries)

