from flask import Flask, jsonify, render_template, request
import psycopg2
from psycopg2.extras import RealDictCursor
from collections import defaultdict
import pandas as pd
import numpy as np

app = Flask(__name__)

# Verbindung zur PostgreSQL-Datenbank
def get_db_connection():
    connection = psycopg2.connect(
        dbname='reviews_db',  # Ersetze mit deinem DB-Namen
        user='scraping_user',    # Ersetze mit deinem DB-Nutzer
        password='Passwort123', # Ersetze mit deinem DB-Passwort
	host='localhost'
	)
    return connection


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/restaurants/<bounds>', methods=['POST'])
def get_restaurants(bounds):
    try:
        south, north, west, east = map(float, bounds.split(','))
    except ValueError:
        return jsonify({"error": "Invalid bounds format. Use: south,north,west,east"}), 400

    buffer = 0.03
    south -= buffer
    north += buffer
    west -= buffer
    east += buffer

    filter_data = request.get_json()
    overall_filter = filter_data.get("general", 0)
    food_filter = filter_data.get("food", 0)

    connection = get_db_connection()
    cursor = connection.cursor(cursor_factory=RealDictCursor)

    # SQL-Abfrage mit den bounds filtern
    cursor.execute("""
        SELECT 
            rb.restaurant_id, 
            rb.city_id, 
            rb.lat_value, 
            rb.long_value, 
            rg.google_rating
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
        max_per_city = 400 // num_cities
    else:
        max_per_city = 400

    # Filtere Restaurants, so dass es maximal max_per_city pro Stadt gibt
    filtered_restaurants = []
    for _, city_restaurants in cities.items():
        filtered_restaurants.extend(city_restaurants[:max_per_city])

    filtered_restaurants = pd.DataFrame(filtered_restaurants)

    def check_filtered(row):
        return 1 if row["google_rating"] >= overall_filter else 0

    filtered_restaurants["filtered"] = filtered_restaurants.apply(check_filtered, axis=1)

    filtered_restaurants = filtered_restaurants.to_dict(orient="records")
    
    # Converting NaN to none as json cannot handle NaN
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
            "website_uri": restaurant_data["website_uri"] or np.nan,
            "opening_hours": restaurant_data["opening_hours"] or np.nan,
            "summary_overall": restaurant_data["summary_overall"] or np.nan,
            "summary_food": restaurant_data["summary_food"] or np.nan,
            "summary_service": restaurant_data["summary_service"] or np.nan,
            "summary_atmosphere": restaurant_data["summary_atmosphere"] or np.nan,
            "summary_price": restaurant_data["summary_price"] or np.nan,
            "rating_overall": restaurant_data["google_rating"] or np.nan,
            "rating_food": restaurant_data["rating_food"] or np.nan,
            "rating_service": restaurant_data["rating_service"] or np.nan,
            "rating_atmosphere": restaurant_data["rating_atmosphere"] or np.nan,
            "rating_price": restaurant_data["rating_price"] or np.nan,
            "user_count_overall_google": restaurant_data["google_user_rating_count"] or np.nan,
            "user_count_overall": restaurant_data["user_count_overall"] or np.nan,
            "user_count_food": restaurant_data["user_count_food"] or np.nan,
            "user_count_service": restaurant_data["user_count_service"] or np.nan,
            "user_count_atmosphere": restaurant_data["user_count_atmosphere"] or np.nan,
            "user_count_price": restaurant_data["user_count_price"] or np.nan
        }

        # Gebe die Informationen als JSON zurück
        return jsonify(restaurant)

    finally:
        # Cursor und Verbindung schließen
        cursor.close()
        connection.close()






if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8080)
