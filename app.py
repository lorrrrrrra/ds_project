from flask import Flask, jsonify, render_template, request
import psycopg2
from psycopg2.extras import RealDictCursor
from collections import defaultdict
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from datetime import datetime

app = Flask(__name__)

# Connection to database
def get_db_connection():
    connection = psycopg2.connect(
        dbname='reviews_db',  
        user='scraping_user',    
        password='Passwort123', 
	host='localhost'
	)
    return connection


# function to calculate the actual date
def calculate_actual_date(row):
    review_text = row["review_date"]
    scraping_date = row["scraping_date"]

    # check if review date is a string
    if not isinstance(review_text, str) or pd.isna(review_text):
        return None

    # months
    if "Monat" in review_text:
        months = int(review_text.split()[1]) if "einem" not in review_text else 1
        return scraping_date - relativedelta(months=months)

    # years
    elif "Jahr" in review_text:
        years = int(review_text.split()[1]) if "einem" not in review_text else 1
        return scraping_date - relativedelta(years=years)

    # weeks
    elif "Woche" in review_text:
        weeks = int(review_text.split()[1]) if "einer" not in review_text else 1
        return scraping_date - pd.to_timedelta(weeks * 7, unit="days")

    # days
    elif "Tag" in review_text:
        days = int(review_text.split()[1]) if "einem" not in review_text else 1
        return scraping_date - pd.to_timedelta(days, unit="days")

    # default if nothing is found
    return None





@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/restaurants/<bounds>', methods=['POST'])
def get_restaurants(bounds):
    try:
        south, north, west, east = map(float, bounds.split(','))
    except ValueError:
        return jsonify({"error": "Invalid bounds format. Use: south,north,west,east"}), 400

    # buffer = 0.03
    south -= south*0.00005
    north += north*0.00005
    west -= west*0.0001
    east += east*0.0001

    print(south, north, west, east)

    filter_data = request.get_json()

    connection = get_db_connection()
    cursor = connection.cursor(cursor_factory=RealDictCursor)

    # Getting all restaurants determine by the bounds given in the api call
    cursor.execute("""
        SELECT 
            rb.restaurant_id, 
            rb.city_id, 
            rb.lat_value, 
            rb.long_value, 
            rb.types,
            rg.google_rating,
            rg.rating_food,
            rg.rating_service,
            rg.rating_atmosphere,
            rg.rating_price
        FROM restaurant_basics rb
        JOIN restaurant_general rg ON rb.restaurant_id = rg.restaurant_id
        WHERE rb.lat_value BETWEEN %s AND %s
        AND rb.long_value BETWEEN %s AND %s;
    """, (south , north, west, east))


    restaurants = cursor.fetchall()

    # Restaurants nach city_id gruppieren
    cities = defaultdict(list)
    for restaurant in restaurants:
        cities[restaurant['city_id']].append(restaurant)

    # Bestimme die Anzahl der Städte
    num_cities = len(cities)

    # Berechne die maximale Anzahl von Restaurants pro Stadt
    if num_cities > 1:
        max_per_city = 600 // num_cities
    else:
        max_per_city = 600

    # Filtere Restaurants, so dass es maximal max_per_city pro Stadt gibt
    filtered_restaurants = []
    for _, city_restaurants in cities.items():
        filtered_restaurants.extend(city_restaurants[:max_per_city])


    # Converting to dataframe and NaN to none
    filtered_restaurants = pd.DataFrame(filtered_restaurants)
    filtered_restaurants = filtered_restaurants.replace({pd.NA: np.nan})


    def check_filtered(row):
        # Hole die Filter-Typen und Restaurant-Typen
        filter_types = filter_data.get("type", [])
        restaurant_types_str = row["types"]
    
        # Falls der Restaurant-Typ als String mit Listendarstellung vorliegt, in eine echte Liste umwandeln
        if isinstance(restaurant_types_str, str):
            # Entferne die eckigen Klammern und teile den String anhand von ', '
            restaurant_types = restaurant_types_str.strip("[]").replace("'", "").split(", ")
        else:
            restaurant_types = restaurant_types_str


        if isinstance(row["rating_price"], str):
            cleaned_price = row["rating_price"].replace("–", "").strip()
        else:
            cleaned_price = row["rating_price"]

        filter_data["price"] = [x.strip() for x in filter_data["price"]]

        conditions = [
            (row["google_rating"] >= filter_data.get("general", 0)) if not np.isnan(row["google_rating"]) else True,                # general rating 
            (row["rating_food"] >= filter_data.get("food", 0)) if not np.isnan(row["rating_food"]) else True,
            (row["rating_service"] >= filter_data.get("service", 0)) if not np.isnan(row["rating_service"]) else True,
            (row["rating_atmosphere"] >= filter_data.get("atmosphere", 0)) if not np.isnan(row["rating_atmosphere"]) else True,
            set(filter_types).issubset(set(restaurant_types)),          # checking if all the types we filter on are in the types list from the restaurant
            (cleaned_price in filter_data["price"]) if filter_data.get("price") else True     # checking whether the price range is in one of the price ranges in the filter
        ]
        return True if all(conditions) else False

    filtered_restaurants["filtered"] = filtered_restaurants.apply(check_filtered, axis=1)

    filtered_restaurants = filtered_restaurants.to_dict(orient="records")
    filtered_restaurants = [{k: (None if isinstance(v, float) and np.isnan(v) else v) for k, v in item.items()} for item in filtered_restaurants]
    



    cursor.close()
    connection.close()

    # Ergebnisse als JSON zurückgeben
    return jsonify(filtered_restaurants)




@app.route('/api/restaurant/<restaurant_id>', methods=['GET'])
def get_restaurant(restaurant_id):
    connection = get_db_connection()
    cursor = connection.cursor(cursor_factory=RealDictCursor)

    try:
        # Eine kombinierte SQL-Abfrage, die alle relevanten Daten aus beiden Tabellen holt
        cursor.execute("""
            SELECT r.name, r.address, r.types,
                rg.website_uri, rg.opening_hours, 
                rg.summary_overall, rg.summary_food, rg.summary_service, rg.summary_atmosphere, rg.summary_price,
                rg.google_rating, rg.rating_food, rg.rating_service, rg.rating_atmosphere, rg.rating_price, 
                rg.google_user_rating_count, rg.user_count_overall, rg.user_count_food, rg.user_count_service, rg.user_count_atmosphere, rg.user_count_price
            FROM restaurant_basics r
            LEFT JOIN restaurant_general rg ON r.restaurant_id = rg.restaurant_id
            WHERE r.restaurant_id = %s;
        """, (restaurant_id,))
        restaurant_data = cursor.fetchone()

        if not restaurant_data:
            # Falls keine Daten gefunden wurden, gebe eine Fehlermeldung zurück
            return jsonify({"error": "Restaurant not found"}), 404

        # Restaurantinformationen
        restaurant = {
            "name": restaurant_data["name"],
            "address": restaurant_data["address"],
            "types": restaurant_data["types"],
            "website_uri": restaurant_data["website_uri"] or None,
            "opening_hours": restaurant_data["opening_hours"] or None,
            "summary_overall": restaurant_data["summary_overall"] or None,
            "summary_food": restaurant_data["summary_food"] or None,
            "summary_service": restaurant_data["summary_service"] or None,
            "summary_atmosphere": restaurant_data["summary_atmosphere"] or None,
            "summary_price": restaurant_data["summary_price"] or None,
            "rating_overall": restaurant_data["google_rating"] or None,
            "rating_food": restaurant_data["rating_food"] or None,
            "rating_service": restaurant_data["rating_service"] or None,
            "rating_atmosphere": restaurant_data["rating_atmosphere"] or None,
            "rating_price": restaurant_data["rating_price"] or None,
            "user_count_overall_google": restaurant_data["google_user_rating_count"] or None,
            "user_count_overall": restaurant_data["user_count_overall"] or None,
            "user_count_food": restaurant_data["user_count_food"] or None,
            "user_count_service": restaurant_data["user_count_service"] or None,
            "user_count_atmosphere": restaurant_data["user_count_atmosphere"] or None,
            "user_count_price": restaurant_data["user_count_price"] or None
        }

        # Gebe die Informationen als JSON zurück
        return jsonify(restaurant)

    finally:
        # Cursor und Verbindung schließen
        cursor.close()
        connection.close()


@app.route('/api/avg_price/<restaurant_id>', methods=['GET'])
def get_price_data_graph(restaurant_id):
    connection = get_db_connection()
    cursor = connection.cursor(cursor_factory=RealDictCursor)

    def count_price_ranges_with_all(df, all_price_ranges):
        # counting the price ranges and returning a df
        price_range_counts = df["dining_price_range"].value_counts()
        price_range_counts = price_range_counts.reindex(all_price_ranges["dining_price_range"]).fillna(0)
        price_range_counts_df = price_range_counts.reset_index()
        price_range_counts_df.columns = ["dining_price_range", "dining_price_range_count"]
        price_range_counts_df["dining_price_range"] = price_range_counts_df["dining_price_range"].replace("Mehr als 100\xa0€", "More than 100\xa0€")
        
        return price_range_counts_df
    

    all_price_ranges = ['1–10\xa0€', '10–20\xa0€', '20–30\xa0€', '30–40\xa0€', '40–50\xa0€', '50–60\xa0€', '60–70\xa0€', '70–80\xa0€', '80–90\xa0€', '90–100\xa0€', 'Mehr als 100\xa0€', None]
    all_price_ranges = pd.DataFrame(all_price_ranges, columns=["dining_price_range"])

    try:
        cursor.execute("""
            SELECT dining_price_range
            FROM reviews_additional
            WHERE restaurant_id = %s;
        """, (restaurant_id,))
        price_range_data = cursor.fetchall()

        

        if not price_range_data:
            # Falls keine Daten gefunden wurden, gebe eine Fehlermeldung zurück
            return jsonify({"error": "Restaurant not found"}), 404

        price_range_data = [row['dining_price_range'] for row in price_range_data if row['dining_price_range'] is not None]
        print(f"{price_range_data}")
        price_range_data_df = pd.DataFrame(price_range_data, columns=["dining_price_range"])

        result = count_price_ranges_with_all(price_range_data_df, all_price_ranges)

        
        # Gebe die Informationen als JSON zurück
        return jsonify(result.to_json(orient="records"))

    finally:
        # Cursor und Verbindung schließen
        cursor.close()
        connection.close()





@app.route('/api/avg_<category>/<restaurant_id>', methods=['GET'])
def get_category_data_graph(restaurant_id, category):
    # we only have data for the three categories 
    if category not in ("food", "service", "atmosphere"):
        return jsonify({"error": "Category not found"}), 404
    
    connection = get_db_connection()
    cursor = connection.cursor(cursor_factory=RealDictCursor)

    # Mapping der Kategorien zu den richtigen Spaltennamen
    category_column_mapping = {
        'food': 'rating_food',
        'service': 'rating_service',
        'atmosphere': 'rating_atmosphere'
    }

    if category not in category_column_mapping:
        return jsonify({"error": "Invalid category"}), 400

    category_column = category_column_mapping[category]
    
    try:
        cursor.execute(f"""
            SELECT s.review_id, s.{category_column}, g.review_date, g.scraping_date
            FROM reviews_subcategories s
            LEFT JOIN reviews_general g ON g.review_id = s.review_id
            WHERE s.restaurant_id = %s;
        """, (restaurant_id,))
        category_data = cursor.fetchall()

        if not category_data:
            return jsonify({"error": f"{category.capitalize()} data not found"}), 404

        # Processing the category data
        category_data = [tuple(row.values()) for row in category_data]
        column_names = ['review_id', category_column, 'review_date', 'scraping_date']
        category_data_df = pd.DataFrame(category_data, columns=column_names)

        # Convert scraping_date to datetime
        category_data_df["scraping_date"] = pd.to_datetime(category_data_df["scraping_date"])

        # Apply the function to calculate the actual review date
        category_data_df["actual_review_date"] = category_data_df.apply(calculate_actual_date, axis=1)

        category_data_df["review_month"] = category_data_df["actual_review_date"].dt.strftime("%m/%Y")

        reviews_grouped_month = (
            category_data_df.groupby(["review_month"])
            .agg(
                dining_stars_mean=(category_column, "mean"),
                dining_stars_count=(category_column, "count"),
            )
            .reset_index()
        )

        # Filter data for 2024
        months = ["02/2024", "03/2024", "04/2024", "05/2024", "06/2024", "07/2024",
                  "08/2024", "09/2024", "10/2024", "11/2024", "12/2024"]
        reviews_grouped_month = reviews_grouped_month[reviews_grouped_month['review_month'].isin(months)]

        # deleting rows where count is 0
        reviews_grouped_month = reviews_grouped_month[reviews_grouped_month['dining_stars_count'] > 0]

        # Return the data as JSON
        return jsonify(reviews_grouped_month.to_json(orient="records"))

    finally:
        cursor.close()
        connection.close()



if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8080)
