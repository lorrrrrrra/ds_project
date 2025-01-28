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
    API_general = pd.read_csv("/home/ubuntu/scraping/API_general.csv")
    API_general["price_range"] = API_general["price_range"].apply(make_to_json)  # Sicherstellen, dass JSON korrekt ist
    API_general["opening_hours"] = API_general["opening_hours"].apply(make_to_json)  # Sicherstellen, dass JSON korrekt ist
    API_general = API_general.drop_duplicates(subset=['restaurant_id'])
    print("CSV wurde erfolgreich eingelesen!")
except Exception as e:
    print("Fehler beim Einlesen der CSV:", e)


try: 
    API_basics = pd.read_csv("/home/ubuntu/scraping/API_basics.csv")
    API_basics = API_basics.drop_duplicates(subset=['restaurant_id'])
    API_basics['pure_service_area'] = API_basics['pure_service_area'].astype(bool)
    print("CSV wurde erfolgreich eingelesen!")
except Exception as e:
    print("Fehler beim Einlesen der CSV:", e)


try: 
    API_additional = pd.read_csv("/home/ubuntu/scraping/API_additional.csv")
    API_additional = API_additional.drop_duplicates(subset=['restaurant_id'])
    print("CSV wurde erfolgreich eingelesen!")
except Exception as e:
    print("Fehler beim Einlesen der CSV:", e)



def insert_data_into_table(connection, cursor, table_name, dataframe, columns):
    try:
        # Erstelle das Insert-Statement dynamisch basierend auf den Spalten
        insert_query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({', '.join(['%s'] * len(columns))});
        """
        
        # Bereite die Daten aus dem DataFrame vor
        rows_to_insert = dataframe[columns].values.tolist()
        
        # Füge alle Zeilen auf einmal in die Tabelle ein
        cursor.executemany(insert_query, rows_to_insert)
        connection.commit()  # Änderungen speichern
        print(f"Daten erfolgreich in die Tabelle '{table_name}' eingefügt!")
    
    except Exception as e:
        print(f"Fehler beim Einfügen in Tabelle '{table_name}': {str(e)}")
        connection.rollback()  # Änderungen zurücksetzen






# Reading data
try:
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor()
    
    # Tabelle: API_general
    table_general = "restaurant_general"
    columns_general = [
        "restaurant_id", "phone_number", "website_uri", "price_level", 
        "price_range", "google_rating", "google_user_rating_count"
    ]
    insert_data_into_table(connection, cursor, table_general, API_general, columns_general)
    
    # Tabelle: API_additional
    table_additional = "restaurant_additional"
    columns_additional = [
        "restaurant_id", "curbside_pickup", "delivery", "dine_in", "live_music",
        "outdoor_seating", "reservable", "restroom", "serves_beer",
        "serves_breakfast", "serves_brunch", "serves_cocktails",
        "serves_coffee", "serves_dessert", "serves_dinner", "serves_lunch",
        "serves_vegetarian_food", "serves_wine", "takeout", "allows_dogs",
        "good_for_children", "good_for_groups", "good_for_watching_sports",
        "menu_for_children", "free_parking_lot", "paid_parking_lot",
        "free_street_parking", "paid_street_parking", "free_garage_parking",
        "paid_garage_parking", "valet_parking", "accepts_debit_cards",
        "accepts_credit_cards", "accepts_cash_only", "accepts_nfc",
        "wheelchair_accessible_restroom", "wheelchair_accessible_entrance",
        "wheelchair_accessible_parking", "wheelchair_accessible_seating"
    ]
    for column in columns_additional[1:]:
        if column in API_additional.columns:
            API_additional[column] = API_additional[column].astype(bool)
            
    insert_data_into_table(connection, cursor, table_additional, API_additional, columns_additional)
    
    # Tabelle: API_basics (Beispiel, falls sie noch ergänzt werden muss)
    table_basics = "restaurant_basics"
    columns_basics = ['restaurant_id', 'city_id', 'name', 'primary_type', 'types', 
                      'business_status', 'pure_service_area', 'address', 'lat_value', 'long_value']
    insert_data_into_table(connection, cursor, table_basics, API_basics, columns_basics)
    
except Exception as e:
    print(f"Fehler bei der Verbindung zur Datenbank: {str(e)}")

finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()