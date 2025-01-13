import psycopg2
import pandas as pd
import json

# Funktion zur Umwandlung in JSON, wenn nötig
def make_to_json(value):
    # Wenn der Wert NaN oder None ist, setzen wir ihn auf ein leeres JSON-Objekt
    if pd.isna(value):
        return '{}' 
    # Wenn der Wert ein String ist, aber nicht richtig formatiert ist, konvertieren wir ihn in den richtigen JSON-String
    elif isinstance(value, str):
        try:
            # Versuche, den String als JSON zu laden und wieder zu dumpen (um sicherzustellen, dass er korrekt formatiert ist)
            return json.dumps(json.loads(value.replace("'", '"')))
        except json.JSONDecodeError as e:
            # Falls es einen Fehler gibt, geben wir ein leeres JSON zurück
            print("Fehler in make_to_json", e)
            return '{}'
    return json.dumps(value)  # Wenn der Wert bereits ein Python-Objekt ist, dann in JSON umwandeln


# Konfiguration für die Verbindung zur PostgreSQL-Datenbank
db_config = {
    "dbname": "reviews_db",
    "user": "scraping_user",
    "password": "Passwort123",
    "host": "localhost",
    "port": 5432
}




# CSV einlesen
try: 
    API_general = pd.read_csv("/home/ubuntu/test_laura/API_general.csv")
    API_general["price_range"] = API_general["price_range"].apply(make_to_json)  # Sicherstellen, dass JSON korrekt ist
    API_general = API_general.drop_duplicates(subset=['restaurant_id'])
    print("CSV wurde erfolgreich eingelesen!")
except Exception as e:
    print("Fehler beim Einlesen der CSV:", e)




# Verbindung zur Datenbank herstellen
try:
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor()
    
    # Erstelle ein Insert-Statement für alle Zeilen im DataFrame
    insert_query = """
        INSERT INTO restaurant_general (restaurant_id, phone_number, website_uri, price_level, price_range, google_rating, google_user_rating_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    
    # Bereite die Daten vor, um sie in die Datenbank einzufügen
    rows_to_insert = API_general[['restaurant_id', 'phone_number', 'website_uri', 'price_level', 'price_range', 'google_rating', 'google_user_rating_count']].values.tolist()
    
    # Füge alle Zeilen auf einmal in die Tabelle ein
    cursor.executemany(insert_query, rows_to_insert)
    connection.commit()  # Änderungen in der DB speichern
    print("Daten erfolgreich eingefügt!")
    
except Exception as e:
    print("Fehler beim Einfügen der Daten:", e)

# Daten aus der Tabelle abfragen und anzeigen
try: 
    cursor.execute("SELECT * FROM restaurant_general LIMIT 10;")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
except Exception as e:
    print("Fehler beim Abrufen der Daten:", e)

# Verbindung schließen
finally:
    cursor.close()
    connection.close()
