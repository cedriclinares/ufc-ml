from gazpacho import get, Soup
import psycopg2
import time

def get_soup_recur(url, count):
    if count > 3:
        print("Soup failed after 3 tries")
        return None
    try:
        html = get(url)
    except:
        print("Error occurred")
        time.sleep(60)
        return get_soup_recur(url, count + 1)
    else:
        return Soup(html)

def get_soup(url):
    return get_soup_recur(url, 1)

def save_odds_data(data):
    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port= '5432'
        )
        print("Saving odds data {}".format(data))

        # Open a cursor to perform database operations
        cursor = conn.cursor()

        # Construct the SQL query with dynamic values
        query = """
            INSERT INTO fight_odds (
               left_name, right_name, left_odds, right_odds, weight_class, date
            )
            VALUES (
                %(left_name)s, %(right_name)s, %(left_odds)s, %(right_odds)s, %(weight_class)s, %(date)s
            )
        """

        # Execute the query with the provided data
        cursor.execute(query, data)

        # Commit the changes to the database
        conn.commit()

        print("Row inserted successfully!")
    except (Exception, psycopg2.Error) as error:
        print("Error while inserting row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def scrape_fight_odds():

    ufc_odds_url = f'https://www.tapology.com/fightcenter/promotions/1-ultimate-fighting-championship-ufc'
    soup = get_soup(ufc_odds_url)
    cards = soup.find('section', {'class': 'fcListing'}, partial=False, mode='all')
    webpage_url = 'https://www.tapology.com'

    for card in cards[13:20]:
        card_date = card.find('span', {'class': 'datetime'}, partial=False, mode='first').text

        print("card_date {}".format(card_date))

        card_name = card.find('span', {'class': 'name'}, partial=False, mode='first')
        card_url = card_name.find('a').attrs['href']
        print("card_url: {}".format(card_url))
        card_details = get_soup(webpage_url + card_url)
        fights = card_details.find('li', {'class': 'fightCard'}, partial=False, mode='all')
        # print("fight length: {}".format(len(fights)))
        for fight in fights:
            odds_data = {}
            fight_url_container = fight.find('span', {'class': 'billing'}, partial=False, mode='first')
            # print('fight_url_container: {}'.format(fight_url_container))
            fight_url = fight_url_container.find('a').attrs['href']
            print("full fight url: {}".format(webpage_url + fight_url))

            fight_details = get_soup(webpage_url + fight_url)
            fight_names_container = fight_details.find('div', {'class': 'fighterNames'})

            left_name_container = fight_names_container.find('span', {'class': 'left'}, partial=True, mode='first')
            odds_data['left_name'] = left_name_container.find('a').text
            right_name_container = fight_names_container.find('span', {'class': 'right'}, partial=True, mode='first')
            odds_data['right_name'] = right_name_container.find('a').text

            fight_stats = fight_details.find('table', {'class': 'fighterStats'}, partial=True, mode='first')
            odds_row = fight_stats.find('tr', mode='all')
            odds_columns = odds_row[2].find('td', mode='all')

            if odds_columns[2].text != 'Betting Odds':
                continue

            odds_data['left_odds'] = odds_columns[0].text.split(' ')[0]
            odds_data['right_odds'] = odds_columns[4].text.split(' ')[0]

            bout_info = fight_details.find('div', {'class': 'details_with_poster'}, partial=True, mode='first')
            date_container = bout_info.find('li', mode='all')[2]
            date_text = date_container.find('span', mode='first').text.split(' ')[1]
            date_mdy = date_text.split('.')
            date_ymd = date_mdy[2] + '-' + date_mdy[0] + '-' + date_mdy[1]
            odds_data['date'] = date_ymd
            weight_class_container = bout_info.find('li', mode='all')[9]
            odds_data['weight_class'] = weight_class_container.find('span', mode='first').text.split(' ')[0]

            print('odds_data: {}'.format(odds_data))
            save_odds_data(odds_data)
                
scrape_fight_odds()
