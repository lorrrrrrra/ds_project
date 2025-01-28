import psycopg2
import pandas as pd

# Verbindung zur PostgreSQL-Datenbank herstellen
db_config = {
    "dbname": "reviews_db",
    "user": "scraping_user",
    "password": "Passwort123",
    "host": "localhost",
    "port": 5432
}

# Verbindung zur DB und Cursor erstellen
connection = psycopg2.connect(**db_config)
cursor = connection.cursor()

# Die Anzahl der Elemente pro Batch
batch_size = 1000

# Holen Sie sich die minimale und maximale review_id
query = "SELECT MIN(review_id), MAX(review_id) FROM reviews_subcategories;"
cursor.execute(query)
min_review_id, max_review_id = cursor.fetchone()

# In Batches aufteilen und Update ausführen
for start_id in range(min_review_id, max_review_id, batch_size):
    end_id = min(start_id + batch_size - 1, max_review_id)
    
    update_query = """
        UPDATE reviews_subcategories AS sub
        SET restaurant_id = general.restaurant_id
        FROM reviews_additional AS general
        WHERE sub.review_id = general.review_id
        AND sub.review_id BETWEEN %s AND %s
    """
    
    try:
        cursor.execute(update_query, (start_id, end_id))
        connection.commit()
        print(f"Batch {start_id} bis {end_id} erfolgreich aktualisiert.")
    except Exception as e:
        print(f"Fehler bei Batch {start_id} bis {end_id}: {e}")
        connection.rollback()

# Verbindung schließen
cursor.close()
connection.close()
