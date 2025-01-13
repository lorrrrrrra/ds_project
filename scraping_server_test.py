from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from multiprocessing import Pool, cpu_count
import psycopg2
import psycopg2.pool
import time
import json
import sys



# Verbindungspool erstellen
conn_pool = psycopg2.pool.SimpleConnectionPool(
    1, 10,  # Min und Max Verbindungen
    dbname="reviews_db", 
    user="scraping_user", 
    password="Passwort123", 
    host="localhost", 
    port=5432,
    sslmode='disable'
)


# generate google maps links for all resturant_ids
def generate_google_maps_links(dataframe, column_name):

    base_url = "https://www.google.com/maps/place/?q=place_id:"
    
    # Create a new column with the maps link
    dataframe['google_maps_link'] = dataframe[column_name].apply(lambda place_id: f"{base_url}{place_id}")
    
    return dataframe

# function in which the browser is already open and reviews will be fetched
def scroll_and_fetch_reviews(driver, max_scrolls, batch_size, pause_time):
    all_reviews_data = []  # To store all collected data
    panel_xpath = '//*[contains(concat( " ", @class, " " ), concat( " ", "jftiEf", " " ))]'
    no_reviews_batches = 0  # Counter for consecutive batches without `listugcposts`

    try:
        # Attempt to locate the reviews panel
        try:
            panel_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, panel_xpath))
            )
            # print("Found the reviews panel.")
        except Exception as e:
            print("Error: Reviews panel not found. Skipping this restaurant.")
            return all_reviews_data  # Return empty if no panel is found
        
        # Initialize ActionChains
        actions = ActionChains(driver)
        actions.move_to_element(panel_element).click().perform()

        for i in range(0, max_scrolls):
            # Scroll within the panel
            actions.send_keys(Keys.PAGE_DOWN).perform()
            time.sleep(pause_time)

            # Check logs after every batch size
            if i % batch_size == 0:
                # print(f"Checking logs after {i} scrolls...")
                logs = driver.get_log("performance")
                found_reviews = False

                for log_entry in logs:
                    try:
                        message = json.loads(log_entry["message"])
                        if message["message"]["method"] == "Network.responseReceived":
                            response_url = message["message"]["params"]["response"]["url"]
                            request_id = message["message"]["params"]["requestId"]

                            # Check if the URL contains `listugcposts`
                            if "listugcposts" in response_url:
                                found_reviews = True

                                # Attempt to fetch the response body
                                response_body = None
                                retry_attempts = 3
                                for attempt in range(retry_attempts):
                                    try:
                                        response_body = driver.execute_cdp_cmd(
                                            'Network.getResponseBody', {'requestId': request_id}
                                        )
                                        if response_body and 'body' in response_body and response_body['body'].strip():
                                            break
                                    except Exception as e:
                                        print(f"Retry {attempt + 1}/{retry_attempts} failed: {e}")
                                        time.sleep(1)

                                # Validate and process the response body
                                if response_body and 'body' in response_body and response_body['body'].strip():
                                    try:
                                        result_data = json.loads(response_body["body"][4:])  # Strip the prefix
                                        all_reviews_data.append(result_data)
                                        # print("Data successfully added.")
                                    except json.JSONDecodeError as e:
                                        print(f"JSONDecodeError while parsing the response body: {e}")
                                else:
                                    print("No valid body found. Skipping...")
                    except Exception as e:
                        print(f"Error while processing logs: {e}")

                # Update the counter for missing `listugcposts`
                if found_reviews:
                    no_reviews_batches = 0  # Reset the counter if data is found
                else:
                    no_reviews_batches += 1
                    # print(f"No `listugcposts` found in this batch. Counter: {no_reviews_batches}")

                # Break the loop if 20 consecutive batches have no `listugcposts`
                if no_reviews_batches >= 20:
                    print("No new `listugcposts` after 20 consecutive batches. Exiting loop.")
                    break

        # print("Scrolling and data fetching completed.")
        return all_reviews_data

    except Exception as e:
        print(f"Error while scrolling or fetching reviews: {e}")
        return all_reviews_data
    
# opening the browser, and fetching the reviews via the scroll and recht reviews function
def get_restaurant_reviews(url, max_scrolls, batch_size, pause_time):
    # Set up performance logging with headless mode
    options = Options()
    options.add_argument("--headless")  # Enable headless mode
    options.add_argument("--disable-gpu")  # Recommended for headless mode
    options.add_argument("--window-size=1920,1080")  # Optional: define window size for consistency
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    # Start ChromeDriver in headless mode
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    # Load the webpage
    driver.get(url)

    # Consent to cookies
    try:
        wait = WebDriverWait(driver, 10)
        button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@class='VfPpkd-LgbsSe VfPpkd-LgbsSe-OWXEXe-k8QpJ VfPpkd-LgbsSe-OWXEXe-dgl2Hf nCP5yc AjY5Oe DuMIQc LQeN7 XWZjwc']")))
        button.click()
        # print("Clicked consent to cookies.")
    except:
        print("No consent required.")

    # Click the reviews tab
    try:
        wait = WebDriverWait(driver, 20)
        button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[.//div[@class='Gpq6kf fontTitleSmall' and text()='Rezensionen']]")))
        if button.is_displayed() and button.is_enabled():
            button.click()
            # print("Clicked to show reviews.")
            time.sleep(2)
        else:
            print("Button is not clickable.")
    except Exception as e:
        print(f"Error: {e}")
        print("Reviews button not found.")

    # Click to sort via date and not relevance
    try:
        wait = WebDriverWait(driver, 20)
        button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[contains(concat( " ", @class, " " ), concat( " ", "ecceSd", " " ))]//*[contains(concat( " ", @class, " " ), concat( " ", "TrU0dc", " " ))]//*[contains(concat( " ", @class, " " ), concat( " ", "DVeyrd", " " ))]')))
        if button.is_displayed() and button.is_enabled():
            button.click()
            # print("Clicked to show sorting values.")
            action_menu = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="action-menu"]/div[2]')))
            if action_menu.is_displayed() and action_menu.is_enabled():
                action_menu.click()
                # print("Clicked to sort values by date.")
            else:
                print("Couldn't click the action menu... :(")
            time.sleep(2)
        else:
            print("Button is not clickable.")

    except Exception as e:
        print(f"Error while sorting via date {e}")

   

    #scroll and fetch the reviews data
    reviews_data = scroll_and_fetch_reviews(driver, max_scrolls=max_scrolls, batch_size=batch_size, pause_time=pause_time)

    driver.quit()

    return reviews_data

# creating dataframes to save the reviews in 
def create_review_dataframes(amount_reviews):
    """
    Initialize empty DataFrames to hold review data and additional information.
    """
    reviews_df = pd.DataFrame({
        'restaurant_id': [None] * amount_reviews,
        'author': [None] * amount_reviews,
        'review_date': [None] * amount_reviews,
        'scraping_date': [None] * amount_reviews,
        'stars': [None] * amount_reviews,
        'language': [None] * amount_reviews,
        'review_text': [None] * amount_reviews
    })

    reviews_additional_df = pd.DataFrame({
        'restaurant_id': [None] * amount_reviews,
        'author': [None] * amount_reviews,
        'dining_mode': [None] * amount_reviews,
        'dining_meal_type': [None] * amount_reviews,
        'dining_price_range': [None] * amount_reviews,
        'dining_stars_food': [None] * amount_reviews,
        'dining_stars_service': [None] * amount_reviews,
        'dining_stars_atmosphere': [None] * amount_reviews,
        'dining_dish_recommend': [None] * amount_reviews,
        'dining_kid_friendliness': [None] * amount_reviews,
        'dining_recommend_for_vegetarians': [None] * amount_reviews,
        'dining_dish_recommend_veggie': [None] * amount_reviews,
        'dining_veggie_tips': [None] * amount_reviews,
        'dining_parking_space_availability': [None] * amount_reviews,
        'dining_parking_options': [None] * amount_reviews,
        'dining_parking_tips': [None] * amount_reviews,
        'dining_accessibility_tips': [None] * amount_reviews
    })

    return reviews_df, reviews_additional_df

# function to handle the additional information
def get_more_information_details(list_more_information):
    """
    Extract additional review details from the "more information" section.
    """
    details = {
        'restaurant_id': None,
        'author': None,
        'dining_mode': None,
        'dining_meal_type': None,
        'dining_price_range': None,
        'dining_stars_food': None,
        'dining_stars_service': None,
        'dining_stars_atmosphere': None,
        'dining_dish_recommend': None,
        'dining_kid_friendliness': None,
        'dining_recommend_for_vegetarians': None,
        'dining_dish_recommend_veggie': None,
        'dining_veggie_tips': None,
        'dining_parking_space_availability': None,
        'dining_parking_options': None,
        'dining_parking_tips': None,
        'dining_accessibility_tips': None
    }

    if list_more_information is not None:
        for entry in list_more_information:
            key = entry[0][0]
            try:
                match key:
                    case "GUIDED_DINING_MODE":
                        details['dining_mode'] = entry[2][0][0][1]
                    case "GUIDED_DINING_MEAL_TYPE":
                        details['dining_meal_type'] = entry[2][0][0][1]
                    case "GUIDED_DINING_PRICE_RANGE":
                        details['dining_price_range'] = entry[2][0][0][1]
                    case "GUIDED_DINING_FOOD_ASPECT":
                        details['dining_stars_food'] = entry[11][0]
                    case "GUIDED_DINING_SERVICE_ASPECT":
                        details['dining_stars_service'] = entry[11][0]
                    case "GUIDED_DINING_ATMOSPHERE_ASPECT":
                        details['dining_stars_atmosphere'] = entry[11][0]
                    case "GUIDED_DINING_DISH_RECOMMENDATION":
                        try: 
                            details['dining_dish_recommend'] = ", ".join([r[1] for r in entry[3][0]])
                        except Exception as e:
                            print("Error happen in more info dish recommend", e)
                    case "GUIDED_DINING_RECOMMEND_TO_VEGETARIANS":
                        details['dining_recommend_for_vegetarians'] = entry[2][0][0][1]
                    case "GUIDED_DINING_VEGETARIAN_OFFERINGS_INFO":
                        try:
                            details['dining_dish_recommend_veggie'] = ", ".join([r[1] for r in entry[3][0]])
                        except Exception as e:
                            print("Error happen in more info veggie recommend", e)
                    case "GUIDED_DINING_VEGETARIAN_OPTIONS_TIPS":
                        details['dining_veggie_tips'] = entry[10][0]
                    case "GUIDED_DINING_PARKING_SPACE_AVAILABILITY":
                        details['dining_parking_space_availability'] = entry[2][0][0][1]
                    case "GUIDED_DINING_PARKING_OPTIONS":
                        try:
                            details['dining_parking_options'] = ", ".join([r[1] for r in entry[3][0]])
                        except Exception as e:
                            print("Error happen in more info parking options", e)
                    case "GUIDED_DINING_PARKING_TIPS":
                        details['dining_parking_tips'] = entry[10][0]
                    case "GUIDED_DINING_ACCESSIBILITY_TIPS":
                        details['dining_accessibility_tips'] = entry[10][0]
                    case "GUIDED_DINING_KID_FRIENDLINESS_TIPS":
                        details['dining_kid_friendliness'] = entry[10][0]
            except Exception as e:
                print("Error happen in more info", e)
                continue

    return details

# function to extract the reviews (uses create_review_dataframes and get_more_information_details)
def get_data_from_reviews(reviews_data_frame, restaurant_id):
    """
    Extract data from the JSON and populate DataFrames.
    """
    # Calculate the total number of reviews
    amount_reviews = sum(len(r[2]) for r in reviews_data_frame)
    reviews_df, reviews_additional_df = create_review_dataframes(amount_reviews)

    # Populate the DataFrames
    review_index = 0
    for restaurant in reviews_data_frame:
        for review in restaurant[2]:
            try:
                author = review[0][1][4][5][0] if review[0][1][4] and len(review[0][1][4]) > 5 else None
                reviews_df.iloc[review_index] = {
                    'restaurant_id': restaurant_id,
                    'author': review[0][1][4][5][0] if review[0][1][4] and len(review[0][1][4]) > 5 else None,
                    'review_date': review[0][1][6] if len(review[0][1]) > 6 else None,
                    'scraping_date': pd.Timestamp.now().date(),
                    'stars': review[0][2][0][0] if len(review[0][2]) > 0 else None,
                    'language': review[0][2][14][0] if len(review[0][2]) > 14 else None,
                    'review_text': review[0][2][15][0][0] if len(review[0][2]) > 15 and review[0][2][15] else None
                }
            except (IndexError, TypeError, KeyError) as e:
                print(f"Error processing review at index {review_index}: {e} (general info)")

            try:
                additional_details = get_more_information_details(review[0][2][6] if len(review[0][2]) > 6 else [])
                if additional_details is None:
                    additional_details = {}
                additional_details.update({'restaurant_id': restaurant_id, 'author': author})
                reviews_additional_df.iloc[review_index] = additional_details
            except (IndexError, TypeError, KeyError) as e:
                print(f"Error processing review at index {review_index}: {e} (additional info)")
            review_index += 1

    print("Data got extracted")

    return reviews_df, reviews_additional_df


def scrape_single_restaurant(link, restaurant_id, max_scrolls, batch_size, pause_time):
    print("Started scraping restaurant", restaurant_id)
    reviews_data_json = get_restaurant_reviews(url=link, max_scrolls=max_scrolls, batch_size=batch_size, pause_time=pause_time)
    reviews, reviews_additional = get_data_from_reviews(reviews_data_json, restaurant_id)

    try:
        connection = conn_pool.getconn()
        cursor = connection.cursor()
        
            # Insert query for the general reviews
        insert_query_general = """
            INSERT INTO reviews_general_test (restaurant_id, author, review_date, scraping_date, stars, language, review_text)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """

        # Insert query for the additional reviews
        insert_query_additional = """
            INSERT INTO reviews_additional_test (restaurant_id, author, dining_mode, dining_meal_type, dining_price_range, dining_stars_food, dining_stars_service, dining_stars_atmosphere, dining_dish_recommend, dining_kid_friendliness, dining_recommend_for_vegetarians, dining_dish_recommend_veggie, dining_veggie_tips, dining_parking_space_availability, dining_parking_options, dining_parking_tips, dining_accessibility_tips)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        # Convert reviews to list of tuples for insertion
        rows_to_insert_general = reviews[['restaurant_id', 'author', 'review_date', 'scraping_date', 'stars', 'language', 'review_text']].values.tolist()
        rows_to_insert_additional = reviews_additional[['restaurant_id', 'author', 'dining_mode', 'dining_meal_type', 'dining_price_range', 'dining_stars_food', 'dining_stars_service', 'dining_stars_atmosphere', 'dining_dish_recommend', 'dining_kid_friendliness', 'dining_recommend_for_vegetarians', 'dining_dish_recommend_veggie', 'dining_veggie_tips', 'dining_parking_space_availability', 'dining_parking_options', 'dining_parking_tips', 'dining_accessibility_tips']].values.tolist()

        try:
            # Execute insert queries
            cursor.executemany(insert_query_general, rows_to_insert_general)
            connection.commit()
            print("General data was successfully submitted into the database", restaurant_id)
        except Exception as e:
            print(f"Problem with writing data to database (general): {e}")
            connection.rollback()

        try:
            cursor.executemany(insert_query_additional, rows_to_insert_additional)
            connection.commit()
            print("Additional data was successfully submitted into the database", restaurant_id)
        except Exception as e:
            print(f"Problem with writing data to database (Additional): {e}")
            connection.rollback()  # Rollback in case of an error during insert

    finally:
        conn_pool.putconn(connection)
        # print("Connection was closed")


# getting the data from the database with the restaurant_id's
try:
    connection = conn_pool.getconn()  # Hole eine Verbindung aus dem Pool
    cursor = connection.cursor()
    cursor.execute("SELECT restaurant_id, google_user_rating_count FROM restaurant_general LIMIT 12;")
    rows = cursor.fetchall()
    columns = ['restaurant_id', 'google_user_rating_count']
    df_restaurant_ids = pd.DataFrame(rows, columns=columns)
    
    print("Dataframe with restaurant id's was constructed")
except Exception as e:
    print("Fehler beim Abrufen der Daten:", e)
    sys.exit(1)
finally:
    conn_pool.putconn(connection)


 # creating the google maps links
df_restaurant_ids = generate_google_maps_links(df_restaurant_ids, 'restaurant_id')

df_restaurant_ids["google_user_rating_count"].fillna(0, inplace=True)
df_restaurant_ids["google_user_rating_count"] = df_restaurant_ids["google_user_rating_count"].astype(int)

    


if __name__ == "__main__":
    # Define parallel scraping with as many processes as needed
    with Pool(processes=3) as pool:
        # Arguments for each task
        max_scrolls = 100000
        batch_size = 5
        pause_time = 0.2
        tasks = [
            (row.google_maps_link, row.restaurant_id, max_scrolls, batch_size, pause_time)
             for row in df_restaurant_ids.to_records(index=False)
        ]

        # Start the parallel processes using starmap
        pool.starmap(scrape_single_restaurant, tasks)