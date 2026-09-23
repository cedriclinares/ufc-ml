import psycopg2
import os
import unicodedata
from unidecode import unidecode


NAME_ALIASES = {
    'Ian Garry': 'Ian Machado Garry',
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
    'E. Zaleski dos Santos': 'Elizeu Zaleski dos Santos',
    'Kauê Fernandes': 'Kaue Fernandes',
    'Victoria Dudakova': 'Viktoriia Dudakova',
    'Joo Sang Yoo': 'JooSang Yoo',
    'Doo Ho Choi': 'Dooho Choi',
    'Waldo Cortes-Acosta': 'Waldo Cortes Acosta',
    'Meng Ding': 'Ding Meng',
    'Jingnan Xiong': 'Xiong Jingnan',
    'Long Xiao': 'Xiao Long',
    'Ce Liu': 'Liu Ce',
    'Xiaonan Yan': 'Yan Xiaonan',
    'Seok Hyeon Ko': 'Seokhyeon Ko',
    'Cong Wang': 'Wang Cong',
    'Loma': 'Loma Lookboonmee',
    'Kangjie Zhu': 'Zhu Kangjie',
    'Yi Sak Lee': 'YiSak Lee',
    'Mingyang Zhang': 'Zhang Mingyang',
    'Darya Zheleznyakova': 'Daria Zhelezniakova',
    'A. Al-Selwady': 'Abdul-Kareem Al-Selwady',
    'Soo Young Yoo': 'SuYoung You',
    'Su Young You': 'SuYoung You',
    'Long Xiao': 'Xiao Long',
    'D. Silva de Andrade': 'Douglas Silva de Andrade',
    'José Medina': 'Jose Daniel Medina',
    'Sang Uk Kim': 'Sangwook Kim',
    'Raffael Cerqueira': 'Rafael Cerqueira',
    'Chang Ho Lee': 'ChangHo Lee',
    'Darya Zheleznyakova': 'Daria Zhelezniakova',
    'N. Tumendemberel': 'Nyamjargal Tumendemberel',
    'Ariane Lipski': 'Ariane da Silva',
    'Xiaocan Feng': 'Feng Xiaocan',
    'Ming Shi': 'Shi Ming',
    'Dong Hun Choi': 'DongHun Choi',
    'Ovince St. Preux': 'Ovince Saint Preux',
    'Jingliang Li': 'Li Jingliang',
    'Jeong Yeong Lee': 'JeongYeong Lee',
    'M. Waterson-Gomez': 'Michelle Waterson-Gomez',
    'Zach Scroggin': 'Zachary Scroggin',
}


def connect_database():
    """Use the same PG* settings and default database as the Playwright scrapers."""
    return psycopg2.connect(dbname=os.environ.get('PGDATABASE', 'cedriclinares'))


def normalize_fighter_name(name):
    """Normalize Unicode and spacing, apply aliases, then transliterate to ASCII."""
    name = unicodedata.normalize('NFC', name)
    name = ' '.join(name.split())
    name = NAME_ALIASES.get(name, name)
    return ' '.join(unidecode(name).split())


def save_b_fighter_id(data):
    conn = None
    cursor = None
    try:
        conn = connect_database()
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
    except Exception as error:
        print("Error while inserting row:", error)
        raise
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def save_r_fighter_id(data):
    conn = None
    cursor = None
    try:
        conn = connect_database()

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
    except Exception as error:
        print("Error while inserting row:", error)
        raise
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def get_fighter_id(name):
    conn = None
    cursor = None
    try:
        conn = connect_database()
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
    except Exception as error:
        print("Error while getting fighter id:", error)
        raise
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def get_fights_without_fighter_ids():
    conn = None
    cursor = None
    try:
        conn = connect_database()
        cursor = conn.cursor()

        # Construct the SQL query with dynamic values
        query = """
            SELECT id, r_name, b_name FROM fights WHERE r_fighter_id IS NULL;
        """

        # Execute the query with the provided data
        cursor.execute(query)
        fight_info = cursor.fetchall()

        return fight_info
    except Exception as error:
        print("Error while inserting row:", error)
        raise
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def get_fights_without_odds_ids():
    conn = None
    cursor = None
    try:
        conn = connect_database()
        cursor = conn.cursor()

        # Construct the SQL query with dynamic values
        query = """
            SELECT id, r_name, b_name, date, weight FROM fights WHERE fight_odds_id IS NULL;
        """

        # Execute the query with the provided data
        cursor.execute(query)
        fight_info = cursor.fetchall()

        return fight_info
    except Exception as error:
        print("Error while inserting row:", error)
        raise
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def get_fight_for_odds(fight_odds_info):
    print("fight odds info: {}".format(fight_odds_info))
    conn = None
    cursor = None
    try:
        conn = connect_database()
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
    except Exception as error:
        print("Error while fetching row:", error)
        raise
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def get_all_fight_odds():
    # print("fight odds info: {}".format(fight_odds_info))
    conn = None
    cursor = None
    try:
        conn = connect_database()
        cursor = conn.cursor()
        # Construct the SQL query with dynamic values
        query = """
            SELECT * FROM fight_odds WHERE date > '2023-10-07' AND fight_id IS NULL
        """

        # Execute the query with the provided data
        cursor.execute(query)
        odds_info = cursor.fetchall()

        return odds_info
    except Exception as error:
        print("Error while fetching row:", error)
        raise
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def save_fight_odds(odds_with_corner):
    print("odds_with_corner: {}".format(odds_with_corner))
    conn = None
    cursor = None
    try:
        conn = connect_database()
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
    except Exception as error:
        print("Error while fetching row:", error)
        raise
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
    left_name = normalize_fighter_name(fight_odds[1])
    right_name = normalize_fighter_name(fight_odds[2])
    r_name = normalize_fighter_name(fight[1])
    b_name = normalize_fighter_name(fight[2])
    
    if (left_name == r_name and right_name == b_name):
        odds_dict = { 'fight_id': fight[0], 'fight_odds_id': fight_odds[0], 'r_fighter_odds': fight_odds[3], 'b_fighter_odds': fight_odds[4]}
    elif left_name == b_name and right_name == r_name:
        odds_dict = { 'fight_id': fight[0], 'fight_odds_id': fight_odds[0], 'r_fighter_odds': fight_odds[4], 'b_fighter_odds': fight_odds[3]}

    else:
        return None

    odds_dict['weight'] = fight_odds[5]
    return odds_dict

def get_fight_odds_ids():
    print("getting fight odds")


    fight_odds_data = get_all_fight_odds()
    for fight_odds in fight_odds_data:
        print('fight odds: {}'.format(fight_odds))
        left_name = normalize_fighter_name(fight_odds[1])
        right_name = normalize_fighter_name(fight_odds[2])
        date = fight_odds[6]
        fight_odds_dict = { 'left_name': left_name, 'right_name': right_name, 'date': date}
        fight_data = get_fight_for_odds(fight_odds_dict)
        print('fight data: {}'.format(fight_data))
        if fight_data:
            odds_with_corner = match_corner_to_fight_odds(fight_odds, fight_data)
            if odds_with_corner is None:
                print('COULD NOT MATCH CORNERS FOR', left_name, 'vs', right_name)
                continue
            print('odds with corner: {}'.format(odds_with_corner))
            save_fight_odds(odds_with_corner)
        else:
            print('COULD NOT MATCH FIGHT FOR ', left_name, ' vs ', right_name)

if __name__ == "__main__":
    get_fight_odds_ids()
        
