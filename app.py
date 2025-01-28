from flask import Flask, jsonify, render_template
import psycopg2
from psycopg2.extras import RealDictCursor

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

@app.route('/api/restaurants/<bounds>', methods=['GET'])
def get_restaurants(bounds):
    try:
        south, north, west, east = map(float, bounds.split(','))
    except ValueError:
        return jsonify({"error": "Invalid bounds format. Use: south,north,west,east"}), 400

    connection = get_db_connection()
    cursor = connection.cursor(cursor_factory=RealDictCursor)

    # SQL-Abfrage mit den bounds filtern
    cursor.execute("""
        SELECT restaurant_id, lat_value, long_value
        FROM restaurant_basics
        WHERE lat_value BETWEEN %s AND %s
        AND long_value BETWEEN %s AND %s
        LIMIT 200;
    """, (south, north, west, east))

    # Hole die Ergebnisse
    restaurants = cursor.fetchall()

    cursor.close()
    connection.close()

    # Ergebnisse als JSON zurückgeben
    return jsonify(restaurants)




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
                rg.rating_overall, rg.rating_food, rg.rating_service, rg.rating_atmosphere, rg.rating_price, 
                rg.user_count_overall, rg.user_count_food, rg.user_count_service, rg.user_count_atmosphere, rg.user_count_price
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
            "rating_overall": restaurant_data["rating_overall"] or None,
            "rating_food": restaurant_data["rating_food"] or None,
            "rating_service": restaurant_data["rating_service"] or None,
            "rating_atmosphere": restaurant_data["rating_atmosphere"] or None,
            "rating_price": restaurant_data["rating_price"] or None,
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
