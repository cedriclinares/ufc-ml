from gazpacho import get, Soup
import psycopg2
from datetime import datetime
# from dateutil.parser import parse


def get_soup(url):
    html = get(url)
    return Soup(html)

def save_weightclass(weight_class_data):
    weight = None
    match weight_class_data['weight_class']:
        case 'strawweight':
            weight = 115
        case 'flyweight':
            weight = 125
        case 'bantamweight':
            weight = 135
        case 'featherweight':
            weight = 145
        case 'lightweight':
            weight = 155
        case 'welterweight':
            weight = 170
        case 'middleweight':
            weight = 185
        case 'light heavyweight':
            weight = 205
        case 'heavyweight':
            weight = 265
        case 'catch weight':
            weight = 'catch'

    weight_class_data['weight'] = weight
    print("Saving weight: {}".format(weight_class_data))

    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port= '5432'
        )
        print("Saving fight data {}".format(weight_class_data))

        # Open a cursor to perform database operations
        cursor = conn.cursor()

        # Construct the SQL query with dynamic values
        query = """
            UPDATE fights
            SET weight = %(weight)s
            WHERE ((r_name = %(fighter1_name)s AND b_name = %(fighter2_name)s) OR (r_name = %(fighter2_name)s AND b_name = %(fighter1_name)s)) AND date=%(date)s;
        """

        # Execute the query with the provided data
        cursor.execute(query, weight_class_data)

        # Commit the changes to the database
        conn.commit()

        print("Weight inserted successfully!")
    except (Exception, psycopg2.Error) as error:
        print("Error while inserting row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def scrape_fight_data():
    ufc_cards_url = 'http://ufcstats.com/statistics/events/completed?page=all'
    soup = get_soup(ufc_cards_url)
    cards = soup.find('tr', {'class': 'b-statistics__table-row'}, partial=False, mode='all')
    # cards[4:-27]
    for card in cards[4:-27]:
        card_details_url = card.find('a', {'class': 'b-link_style_black'}).attrs['href']
        card_date = card.find('span', {'class': 'b-statistics__date'}).text
        print('Card date: {}'.format(card_date))
        fights = get_soup(card_details_url)
        fight_details = fights.find('tbody', {'class': 'b-fight-details__table-body'}).find('tr', {'class': 'b-fight-details__table-row'}, partial=True, mode='all')
        fight_winner = ""
       
        for fight in fight_details:
            weight_class_data = {}
            # Input date string
            input_date_str = card_date
            # Parse input date string
            parsed_date = datetime.strptime(input_date_str, '%B %d, %Y')
            # Format the parsed date into the desired output format
            output_date_str = parsed_date.strftime('%Y-%m-%d')

            weight_class_data['date'] = output_date_str
            fight_detail_columns = fight.find('td', {'class': 'b-fight-details__table-col'}, partial=True, mode='all')
            fighter_names_container = fight_detail_columns[1]
            fighter_names = fighter_names_container.find('a', mode='all')
            weight_class_data['fighter1_name'] = fighter_names[0].strip()
            weight_class_data['fighter2_name'] = fighter_names[1].strip()
            weight_class = fight_detail_columns[6].text
            if weight_class.find('Women') != -1:
                weight_class = weight_class.split(' ')[1]
            
            weight_class_data['weight_class'] = weight_class.lower()
            # print('weight class data: {}'.format(weight_class_data))
            save_weightclass(weight_class_data)
            


scrape_fight_data()

