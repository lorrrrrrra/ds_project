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
        dbname='reviews_db',  
        user='scraping_user',    
        password='Passwort123', 
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
        max_per_city = 400 // num_cities
    else:
        max_per_city = 400

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

        
        # Wenn eine der beiden als String vorliegt, umwandeln in eine Liste
        if isinstance(filter_types, str):
            filter_types = [filter_types]
        if isinstance(restaurant_types, str):
            restaurant_types = [restaurant_types]

        print(repr(row["rating_price"]))
        print(repr(filter_data["price"]))
        print((row["rating_price"] in filter_data["price"]))

        conditions = [
            (row["google_rating"] >= filter_data.get("general", 0)) if not np.isnan(row["google_rating"]) else True,                # general rating 
            (row["rating_food"] >= filter_data.get("food", 0)) if not np.isnan(row["rating_food"]) else True,
            (row["rating_service"] >= filter_data.get("service", 0)) if not np.isnan(row["rating_service"]) else True,
            (row["rating_atmosphere"] >= filter_data.get("atmosphere", 0)) if not np.isnan(row["rating_atmosphere"]) else True,
            set(filter_types).issubset(set(restaurant_types)),          # checking if all the types we filter on are in the types list from the restaurant
            (row["rating_price"] in filter_data["price"]) if filter_data.get("price") else True     # checking whether the price range is in one of the price ranges in the filter
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






if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8080)
