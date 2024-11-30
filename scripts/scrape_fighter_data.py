from gazpacho import get, Soup
import psycopg2

def get_soup(url):
    html = get(url)
    return Soup(html)

def save_fighter_data(data):
    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port= '5432'
        )
        # print("Saving fighter data {}".format(data))

        # Open a cursor to perform database operations
        cursor = conn.cursor()

        # Construct the SQL query with dynamic values
        query = """
            INSERT INTO fighters (
               name, weight, height, reach, stance, date_of_birth, gender 
            )
            VALUES (
                %(name)s, %(weight)s, %(height)s, %(reach)s, %(stance)s,
                %(date_of_birth)s, %(gender)s
            )
            RETURNING id;
        """

        # Execute the query with the provided data
        cursor.execute(query, data)
        fighter_id = cursor.fetchone()[0]
        # Commit the changes to the database
        conn.commit()

        print("Row inserted successfully!")
        return fighter_id
    except (Exception, psycopg2.Error) as error:
        print("Error while inserting row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def get_fighter_attributes(fighter_details, fighter_attributes):
    # print("details: ", fighter_details)
    height = fighter_details[0].html.split('</i>')[1].split('\n')[1].strip()
    weight = fighter_details[1].html.split('</i>')[1].split('\n')[1].strip()
    reach = fighter_details[2].html.split('</i>')[1].split('\n')[1].strip()
    stance = fighter_details[3].html.split('</i>')[1].split('\n')[1].strip()

    if height.find('&quot;') == -1:
        height = None
    if weight.find("lbs") == -1:
        weight = None
    if reach.find('&quot;') == -1:
        reach = None
    if len(stance) == 0 or stance == '</li>':
        stance = None

    if height == None or weight == None or stance == None: 
        return 0
   
    height_feet = height.split(' ')[0][:-1]
    height_inches = height.split(' ')[1][:-6]
    if reach is None:
        reach = int(height_feet)*12 + int(height_inches)
    else: 
        reach = reach[:-6]

    fighter_attributes["height"] = int(height_feet)*12 + int(height_inches)
    fighter_attributes["weight"] = weight[:-5]
    fighter_attributes["reach"] = reach
    fighter_attributes["stance"] = stance
    fighter_attributes["date_of_birth"] = fighter_details[4].html.split('</i>')[1].split('\n')[1].strip()


def collect_fighter_data(fighter_url): 
    print('collecting data {}'.format(fighter_url))
    fighter_page = get_soup(fighter_url)
    fighter_attributes = {}
    fighter_stats_container = fighter_page.find('div', {'class': 'b-list__info-box_style_small-width'}, partial=True, mode='first')
    fighter_stats = fighter_stats_container.find('li', {'class': 'b-list__box-list-item'}, partial=True, mode='all')

    fighter_attributes["name"] = fighter_page.find('span', {'class': 'b-content__title-highlight'}, partial=False, mode='first').text
    fighter_info = fighter_page.find('ul', {'class': 'b-list__box-list'}, partial=False, mode='first')
    fighter_details = fighter_info.find('li', {'class': 'b-list__box-list-item'}, partial=True, mode='all')
    # print("getting fighter attributes")
    fighter_attributes_result = get_fighter_attributes(fighter_details, fighter_attributes)
    print('fighter_attributes_result: {}'.format(fighter_attributes_result))
    fight_list = fighter_page.find('tbody', {'class': 'b-fight-details__table-body'}, partial=False, mode='first')
    first_fight = fight_list.find('tr', {'class': 'b-fight-details__table-row__hover'}, partial=True, mode='first')
    if first_fight == None or fighter_attributes_result == 0:
        return None
    fight_url = first_fight.attrs['data-link']
    fight_page = get_soup(fight_url)
    weight_class = fight_page.find('i', {'class': 'b-fight-details__fight-title'}, partial=True, mode='first').text

    gender = 'male'
    if weight_class.find('Women') != -1:
        gender = 'female'
    fighter_attributes["gender"] = gender
    return fighter_attributes

def scrape_fighter_data(fighter_url):
    fighter_attributes = collect_fighter_data(fighter_url)
    fighterId = None
    if fighter_attributes is not None:
        print("SAVING NEW FIGHTER: ", fighter_attributes)
        fighterId = save_fighter_data(fighter_attributes)
    return fighterId

def get_missing_fighters():
    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port='5432'
        )
        cursor = conn.cursor()

        # Construct the SQL query with dynamic values
        query = """
            SELECT DISTINCT r_name FROM fights WHERE r_fighter_id IS NULL
        """

        # Execute the query with the provided data
        cursor.execute(query)
        fighter_names = cursor.fetchall()

        return fighter_names
    except (Exception, psycopg2.Error) as error:
        print("Error while inserting row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def fill_in_missing_fighters():
    missing_fighters = get_missing_fighters()
    print('missing fighters: {}'.format(missing_fighters))
    for fighter_name in missing_fighters:
        last_name = fighter_name[0].split(' ')[-1]
        ufc_fighters_url = f'http://ufcstats.com/statistics/fighters/search?query={last_name}'
        print('fighter url: {}'.format(ufc_fighters_url))
        soup = get_soup(ufc_fighters_url)
        fighters = soup.find('tr', {'class': 'b-statistics__table-row'}, partial=False, mode='all')
        print('looking for: {}'.format(fighter_name[0]))
        for fighter in fighters[2:]:
            # print('fighter: {}'.format(fighter))
            first_name = fighter.find('td', mode='all')[0].text
            last_name = fighter.find('td', mode='all')[1].text
            full_name = first_name + ' ' + last_name
            print('full name: {}'.format(full_name))
            if (full_name.lower() == fighter_name[0].lower()):
                print('found matching name: {}'.format(full_name))
                fighter_url = fighter.find('a', mode='first').attrs['href']
                fighter_attributes = collect_fighter_data(fighter_url)
                print("attributes: {}".format(fighter_attributes))
                if fighter_attributes != None:
                    save_fighter_data(fighter_attributes)

# fill_in_missing_fighters()
# scrape_fighter_data()