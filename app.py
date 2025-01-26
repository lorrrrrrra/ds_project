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

@app.route('/api/restaurants', methods=['GET'])
def get_restaurants():
    connection = get_db_connection()
    cursor = connection.cursor(cursor_factory=RealDictCursor)
    
    # SQL-Abfrage, um die ersten 5 Zeilen der gewünschten Spalten aus restaurant_basics zu holen
    cursor.execute("""
        SELECT restaurant_id, lat_value, long_value
        FROM restaurant_basics
        LIMIT 5;
    """)
    
    # Hole die Ergebnisse
    restaurants = cursor.fetchall()
    
    cursor.close()
    connection.close()
    
    # Gebe die Ergebnisse als JSON zurück
    return jsonify(restaurants)


@app.route('/api/restaurant/<restaurant_id>', methods=['GET'])
def get_restaurant(restaurant_id):
    connection = get_db_connection()
    cursor = connection.cursor(cursor_factory=RealDictCursor)
    
    try:
        # SQL-Abfrage, um die Informationen des spezifischen Restaurants aus restaurant_basics zu holen
        cursor.execute("""
            SELECT name, address
            FROM restaurant_basics
            WHERE restaurant_id = %s;
        """, (restaurant_id,))
        restaurant = cursor.fetchone()
        
        if not restaurant:
            # Falls kein Restaurant gefunden wurde, gebe eine Fehlermeldung zurück
            return jsonify({"error": "Restaurant not found"}), 404
        
        # Zusätzliche SQL-Abfrage, um die Website-URI aus restaurant_general zu holen
        cursor.execute("""
            SELECT website_uri
            FROM restaurant_general
            WHERE restaurant_id = %s;
        """, (restaurant_id,))
        website_data = cursor.fetchone()
        
        # Falls eine Website gefunden wurde, füge sie den Restaurantinformationen hinzu
        if website_data and "website_uri" in website_data:
            restaurant["website_uri"] = website_data["website_uri"]
        else:
            restaurant["website_uri"] = None  # Wenn keine Website gefunden wurde, setze den Wert auf None
        
        # Gebe die Informationen als JSON zurück
        return jsonify(restaurant)
    
    finally:
        # Cursor und Verbindung schließen
        cursor.close()
        connection.close()





if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8080)
