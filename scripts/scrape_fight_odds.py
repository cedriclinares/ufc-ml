from gazpacho import get, Soup
import psycopg2
import time
from datetime import date
# import requests

def get_soup_recur(url, count):
    if count > 3:
        print("Soup failed after 3 tries")
        return None
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/150.0.0.0 Safari/537.36"
            )
        }
        '''
            response = requests.get(url, headers=headers)
            print(response.status_code)
            print(response.text[:500])
        '''
        html = get(url, headers)
    except Exception as e:
        print(f"Error occurred: {e}")
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

    ufc_odds_url = f'https://www.tapology.com/search?term=ufc&commit=Submit&model%5Bevents%5D=eventsSearch'
    soup = get_soup(ufc_odds_url)

    print('soup', soup)
    # cards = soup.find('section', {'class': 'fcListing'}, partial=False, mode='all')
    table = soup.find("table", class_="fcLeaderboard")
    cards = table.find_all("tr")
    webpage_url = 'https://www.tapology.com'

    for card in cards[1:]:
        card_rows=card.find_all("td")
        card_date = card_rows[2].text
        card_date_formatted = card_date.split(".").join('-')
        card_datetime = datetime.strptime(card_date, "%Y.%m.%d").date()
        curr_datetime = date.today
        last_scraped_card_datetime = datetime.strptime("2023-12-10", "%Y-%m-%d").date()

        card_name = card_rows[0].text
        pattern = r"^(UFC \d+|UFC Fight Night)"

        if card_date_formatted > curr_datetime or card_date_formatted < last_scraped_card_datetime:
            continue

        print("card_date {}".format(card_date))

        if not re.match(pattern, card_name):
            continue

        card_url = card_rows[0].find('a').attrs['href']
        print("card_url: {}".format(card_url))
        time.sleep(6)
        card_details = get_soup(webpage_url + card_url)
        fights = card_details.find_all('div', {'data-bout-wrapper': True})
        # print("fight length: {}".format(len(fights)))
        for fight in fights:
            odds_data = {}

            fighter_names_container = fight.find_all('div', recursive=false)[1]
            left_fighter_name_container = fighter_names_container.find_all('div', recursive=false)[0]
            left_fighter_name = left_fighter_name_container.find('a', {'class': 'link-primary-red'}).text
            right_fighter_name_container = fighter_names_container.find_all('div', recursive=false)[2]
            right_fighter_name = right_fighter_name_container.find('a', {'class': 'link-primary-red'}).text

            fight_weight = fighter_names_container[1].find('span', {'class': 'bg-tap_darkgold'}).text

            details_table = fight.find('table', {'id': 'boutComparisonTable'})
            details_table_rows = details_table.find_all('tr')
            fight_odds_row = details_table_rows[1]
            fight_odds_columns = fight_odds_row.find_all('td')
            left_odds_text = fight_odds_columns[0].text.split(' ')[0]
            right_odds_text = fight_odds_columns[4].text.split(' ')[0]

            odds_data['left_name'] = left_fighter_name
            odds_data['left_odds'] - left_odds_text

            odds_data['right_name'] = right_fighter_name
            odds_data['right_odds'] - right_odds_text

            odds_data['weight_class'] = fight_weight
            odds_data['date'] = card_date_formatted


            '''fight_url_container = fight.find('span', {'class': 'billing'}, partial=False, mode='first')
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
            '''

            print('odds_data: {}'.format(odds_data))
            # save_odds_data(odds_data)
                
scrape_fight_odds()
