from flask import Flask, jsonify
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
    return "Hallo, Flask mit PostgreSQL!"

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

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
