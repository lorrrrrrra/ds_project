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
    
    # SQL-Abfrage, um die Informationen des spezifischen Restaurants zu holen
    cursor.execute("""
        SELECT name, address
        FROM restaurant_basics
        WHERE restaurant_id = %s;
    """, (restaurant_id,))
    
    # Ergebnis abrufen
    restaurant = cursor.fetchone()
    
    cursor.close()
    connection.close()
    
    if restaurant:
        # Gebe die Informationen als JSON zurück
        return jsonify(restaurant)
    else:
        # Falls kein Restaurant mit der gegebenen ID existiert, gebe eine Fehlermeldung zurück
        return jsonify({"error": "Restaurant not found"}), 404





if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8080)
