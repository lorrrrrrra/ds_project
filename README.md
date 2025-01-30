# MapYourRestaurants

## Overview
MapYourRestaurants is a data science project that retrieves and processes restaurant information and reviews to provide customer insights. Using Google APIs, web scraping, and OpenAI's language models, we extract, analyze, and summarize restaurant reviews to help users make informed dining choices.

## Approach
1. Restaurant Data Collection: Using Google APIs the Geocoding API and the Google Places (new) API gathered information about routhly 8,000 restaurants in South Germany (München, Stuttgart, Nürnberg, Mannheim, Karlsruhe, Augsburg, Freiburg, Heidelberg, Regensburg, Tübingen)
2. Review Data Collection: Web scraping of restaurant reviews from Google Maps.
3. Natural Language Processing: (using GPT-4o mini)
  -  Structured topic extraction (for food, service, price, atmosphere)
  -  Sentiment analysis (subratings for the categories from 1 (very bad) to 5 (very good))
  -  Summarization of review texts
4. Storage & Querying: PostgreSQL database

## Website
Go to our website to explore MapYourRestaurants: http://193.196.55.120/ 

## Code
This GitHub repository hosts all the code from data acquisition, processing to website source code.

-------------------------------------
  Contributors

    Theresa Baumstark
    Laura Gaddum
    Juliana Tonn

    This project is part of the "Data Science Project 2024/25" module at the University of Tuebingen.
