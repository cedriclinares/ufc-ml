import psycopg2
from unidecode import unidecode
from gazpacho import get, Soup

def save_b_fighter_id(data):
    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port= '5432'
        )
        cursor = conn.cursor()

        query = """
            UPDATE fights
            SET b_fighter_id = %(b_fighter_id)s
            WHERE id = %(id)s;
        """

        # Execute the query with the provided data
        cursor.execute(query, data)

        # Commit the changes to the database
        conn.commit()

        print("b_fighter_id inserted successfully!")
    except (Exception, psycopg2.Error) as error:
        print("Error while inserting row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def save_r_fighter_id(data):
    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port= '5432'
        )

        # Open a cursor to perform database operations
        cursor = conn.cursor()

        # Construct the SQL query with dynamic values
        query = """
            UPDATE fights
            SET r_fighter_id = %(r_fighter_id)s
            WHERE id = %(id)s;
        """

        # Execute the query with the provided data
        cursor.execute(query, data)

        # Commit the changes to the database
        conn.commit()

        print("r_fighter_id inserted successfully!")
    except (Exception, psycopg2.Error) as error:
        print("Error while inserting row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def get_fighter_id(name):
    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port='5432'
        )
        cursor = conn.cursor()

        # Construct the SQL query with dynamic values
        query = """
            SELECT id FROM fighters WHERE name = %(name)s;
        """

        # Execute the query with the provided data
        cursor.execute(query, { 'name': name })
        result = cursor.fetchone()
        fighter_id = None
        if (result is not None):
            fighter_id = result[0]
        # print("fighter_id: {}".format(fighter_id))
        return fighter_id
    except (Exception, psycopg2.Error) as error:
        print("Error while getting fighter id:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def get_fights_without_fighter_ids():
    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port='5432'
        )
        cursor = conn.cursor()

        # Construct the SQL query with dynamic values
        query = """
            SELECT id, r_name, b_name FROM fights WHERE r_fighter_id IS NULL;
        """

        # Execute the query with the provided data
        cursor.execute(query)
        fight_info = cursor.fetchall()

        return fight_info
    except (Exception, psycopg2.Error) as error:
        print("Error while inserting row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def get_fights_without_odds_ids():
    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port='5432'
        )
        cursor = conn.cursor()

        # Construct the SQL query with dynamic values
        query = """
            SELECT id, r_name, b_name, date, weight FROM fights WHERE fight_odds_id IS NULL;
        """

        # Execute the query with the provided data
        cursor.execute(query)
        fight_info = cursor.fetchall()

        return fight_info
    except (Exception, psycopg2.Error) as error:
        print("Error while inserting row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def get_fight_for_odds(fight_odds_info):
    print("fight odds info: {}".format(fight_odds_info))
    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port='5432'
        )
        cursor = conn.cursor()
        # Construct the SQL query with dynamic values
        query = """
            SELECT id, r_name, b_name, date, weight FROM fights WHERE (
                (r_name = %(left_name)s AND b_name = %(right_name)s) OR 
                (r_name = %(right_name)s AND b_name = %(left_name)s)
            ) AND date = %(date)s
        """

        # Execute the query with the provided data
        cursor.execute(query, fight_odds_info)
        odds_info = cursor.fetchone()

        return odds_info
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def get_all_fight_odds():
    # print("fight odds info: {}".format(fight_odds_info))
    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port='5432'
        )
        cursor = conn.cursor()
        # Construct the SQL query with dynamic values
        query = """
            SELECT * FROM fight_odds WHERE date > '2023-10-07' AND fight_id IS NULL
        """

        # Execute the query with the provided data
        cursor.execute(query)
        odds_info = cursor.fetchall()

        return odds_info
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def save_fight_odds(odds_with_corner):
    print("odds_with_corner: {}".format(odds_with_corner))
    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port='5432'
        )
        cursor = conn.cursor()
        # Construct the SQL query with dynamic values
        query1 = """
            UPDATE fights
            SET weight = %(weight)s,
            fight_odds_id = %(fight_odds_id)s,
            r_fighter_odds = %(r_fighter_odds)s,
            b_fighter_odds = %(b_fighter_odds)s
            WHERE id = %(fight_id)s;
        """
         # Execute the query with the provided data
        cursor.execute(query1, odds_with_corner)

        query2 = """
            UPDATE fight_odds
            SET fight_id = %(fight_id)s
            WHERE id = %(fight_odds_id)s;
        """
        cursor.execute(query2, odds_with_corner)

        conn.commit()
        print("Both queries executed successfully")
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

'''
    1) Loop through each fight
    2) search db for each fighter name
    3) update fighter id_s in fights table
'''

def get_fighter_ids():
    fights = get_fights_without_fighter_ids()
    # print("fights: {}".format(fights))
    for fight in fights:
        id = fight[0]
        r_fighter_name = fight[1]
        r_fighter_id = get_fighter_id(r_fighter_name)
        save_r_fighter_id({ 'r_fighter_id': r_fighter_id, 'id': id })
        b_fighter_name = fight[2]
        b_fighter_id = get_fighter_id(b_fighter_name)
        save_b_fighter_id({ 'b_fighter_id': b_fighter_id, 'id': id })
# get_fighter_ids()

def match_corner_to_fight_odds(fight_odds, fight):
    odds_dict = {}
    print('fight_odds: {}'.format(fight_odds))
    # print('fight: {}'.format(fight))
    left_name = fight_odds[1]
    right_name = fight_odds[2]
    r_name = fight[1]
    b_name = fight[2]
    
    if (left_name == r_name and right_name == b_name):
        odds_dict = { 'fight_id': fight[0], 'fight_odds_id': fight_odds[0], 'r_fighter_odds': fight_odds[3], 'b_fighter_odds': fight_odds[4]}
    else: 
        odds_dict = { 'fight_id': fight[0], 'fight_odds_id': fight_odds[0], 'r_fighter_odds': fight_odds[4], 'b_fighter_odds': fight_odds[3]}

    odds_dict['weight'] = fight_odds[5]
    return odds_dict

def get_fight_odds_ids():
    print("getting fight odds")
    name_dict = {
        'Khalil Rountree': 'Khalil Rountree Jr.',
        'Benoit Saint-Denis': 'Benoit Saint Denis',
        'Thiago Moisés': 'Thiago Moises',
        'Kenan Song': 'Song Kenan',
        'Na Liang': 'Liang Na',
        'Seung Woo Choi': 'SeungWoo Choi',
        'Weili Zhang': 'Zhang Weili',
        'Rafael dos Anjos': 'Rafael Dos Anjos',
        'Montserrat Ruiz': 'Montserrat Conejo Ruiz',
        'Asu Almabaev': 'Assu Almabayev',
        'Caolán Loughran': 'Caolan Loughran',
        'Łukasz Brzeski': 'Lukasz Brzeski',
        'Michał Oleksiejczuk': 'Michal Oleksiejczuk',
        'Natália Silva': 'Natalia Silva',
        'Jéssica Andrade': 'Jessica Andrade',
        'Ľudovít Klein': 'Ludovit Klein',
        'Diana Belbiţă': 'Diana Belbita',
        'Mateus Mendonça': 'Mateus Mendonca',
        'Johnny Munoz Jr.': 'Johnny Munoz',
        'Mizuki Inoue': 'Mizuki',
        'Lupita Godinez': 'Loopy Godinez',
        'Edgar Cháirez': 'Edgar Chairez',
        'Da Un Jung': 'Da Woon Jung',
        'Landon Quiñones': 'Landon Quinones',
        'Mike Mathetha': 'Blood Diamond',
        'Hyun Sung Park': 'HyunSung Park',
        'Jun Yong Park': 'JunYong Park',
        'André Muniz': 'Andre Muniz',
        'Yadong Song': 'Song Yadong',
        'Lucie Pudilová': 'Lucie Pudilova',
        'Jiří Procházka': 'Jiri Prochazka',
        'Mateusz Rębecki': 'Mateusz Rebecki',
        'John Castañeda': 'John Castaneda',
        'Elizeu Zaleski': 'Elizeu Zaleski dos Santos',
        'Kauê Fernandes': 'Kaue Fernandes',
        'Victoria Dudakova': 'Viktoriia Dudakova'
    }

    fight_odds_data = get_all_fight_odds()
    for fight_odds in fight_odds_data:
        fight_odds_id = fight_odds[0]
        left_name = fight_odds[1] # ' '.join(unidecode(fight_odds[1]).split('-'))
        if name_dict.get(left_name) is not None:
            left_name = name_dict[left_name]
        right_name = fight_odds[2] # ' '.join(unidecode(fight_odds[2]).split('-'))
        if name_dict.get(right_name) is not None:
            right_name = name_dict[right_name]
        date = fight_odds[6]
        fight_odds_dict = { 'left_name': left_name, 'right_name': right_name, 'date': date}
        fight_data = get_fight_for_odds(fight_odds_dict)
        print('fight data: {}'.format(fight_data))
        if fight_data:
            odds_with_corner = match_corner_to_fight_odds(fight_odds, fight_data)
            print('odds with corner: {}'.format(odds_with_corner))
            save_fight_odds(odds_with_corner)
        else:
            print('COULD NOT MATCH FIGHT FOR ', left_name, ' vs ', right_name)

get_fight_odds_ids()
        
