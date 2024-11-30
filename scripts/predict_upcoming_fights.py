import psycopg2
from unidecode import unidecode
from gazpacho import get, Soup
import pandas as pd
import pickle

def get_soup(url):
    html = get(url)
    return Soup(html)

def get_fighter_ids(r_fighter_name, b_fighter_name):
    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port='5432'
        )
        cursor = conn.cursor()
        # Construct the SQL query with dynamic values
        query1 = """
            SELECT id FROM fighters WHERE name = %(r_fighter_name)s
        """

        # Execute the query with the provided data
        cursor.execute(query1, { 'r_fighter_name': r_fighter_name })
        r_id = cursor.fetchone()
        query2 = """
            SELECT id FROM fighters WHERE name = %(b_fighter_name)s
        """
        cursor.execute(query2, { 'b_fighter_name': b_fighter_name })
        b_id = cursor.fetchone()
        conn.commit()
        # print("Both queries executed successfully: ", r_id, b_id)
        if r_id is not None and b_id is not None:
            return r_id[0], b_id[0]
        return None, None
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def get_last_total_stats(fighter_id):
    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port='5432'
        )
        cursor = conn.cursor()
        # Construct the SQL query with dynamic values
        query = """
            SELECT *
            FROM total_fight_stats
            WHERE fighter_id = %(fighter_id)s
            ORDER BY id DESC
            LIMIT 1;
        """

        # Execute the query with the provided data
        cursor.execute(query, { 'fighter_id': fighter_id })
        total_stats = cursor.fetchone()
        conn.commit()
        # print("last total stats fetched successfully")
        return total_stats[0]
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def join_tables(r_fighter_id, b_fighter_id, r_total_stats_id, b_total_stats_id):
    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port='5432'
        )
        cursor = conn.cursor()
        # Construct the SQL query with dynamic values
        query = """
            SELECT
            r_fighter.gender AS gender,
            r_stats.age AS r_age,
            r_stats.wins AS r_wins,
            r_stats.losses AS r_losses,
            r_stats.draws AS r_draws,
            r_fighter.stance AS r_stance,
            r_fighter.height AS r_height,
            r_fighter.reach AS r_reach,
            r_stats.opponent_wins AS r_opponent_wins,
            r_stats.opponent_loses AS r_opponent_loses,
            r_stats.championship_fights AS r_championship_fights,
            r_stats.sig_str_landed AS r_sig_str_landed,
            r_stats.sig_str_attempted AS r_sig_str_attempted,
            r_stats.sig_str_absorbed AS r_sig_str_absorbed,
            r_stats.sig_str_evaded AS r_sig_str_evaded,
            r_stats.head_landed AS r_head_landed,
            r_stats.head_attempted AS r_head_attempted,
            r_stats.head_absorbed AS r_head_absorbed,
            r_stats.head_evaded AS r_head_evaded,
            r_stats.body_landed AS r_body_landed,
            r_stats.body_attempted AS r_body_attempted,
            r_stats.body_absorbed AS r_body_absorbed,
            r_stats.body_evaded AS r_body_evaded,
            r_stats.legs_landed AS r_legs_landed,
            r_stats.legs_attempted AS r_legs_attempted,
            r_stats.legs_absorbed AS r_legs_absorbed,
            r_stats.legs_evaded AS r_legs_evaded,
            r_stats.distance_landed AS r_distance_landed,
            r_stats.distance_attempted AS r_distance_attempted,
            r_stats.distance_absorbed AS r_distance_absorbed,
            r_stats.distance_evaded AS r_distance_evaded,
            r_stats.clinch_landed AS r_clinch_landed,
            r_stats.clinch_attempted AS r_clinch_attempted,
            r_stats.clinch_absorbed AS r_clinch_absorbed,
            r_stats.clinch_evaded AS r_clinch_evaded,
            r_stats.ground_landed AS r_ground_landed,
            r_stats.ground_attempted AS r_ground_attempted,
            r_stats.ground_absorbed AS r_ground_absorbed,
            r_stats.ground_evaded AS r_ground_evaded,
            r_stats.total_str_landed AS r_total_str_landed,
            r_stats.total_str_attempted AS r_total_str_attempted,
            r_stats.total_str_absorbed AS r_total_str_absorbed,
            r_stats.total_str_evaded AS r_total_str_evaded,
            r_stats.TD_landed AS r_TD_landed,
            r_stats.TD_attempted AS r_TD_attempted,
            r_stats.TD_absorbed AS r_TD_absorbed,
            r_stats.TD_evaded AS r_TD_evaded,
            r_stats.KD_landed AS r_KD_landed,
            r_stats.KD_absorbed AS r_KD_absorbed,
            r_stats.subs_attempted AS r_subs_attempted,
            r_stats.subs_evaded AS r_subs_evaded,
            r_stats.ctrl_time AS r_ctrl_time,
            r_stats.opponent_ctrl_time AS r_opponent_ctrl_time,
            r_stats.round_1_sig_str_landed AS r_round_1_sig_str_landed,
            r_stats.round_1_sig_str_attempted AS r_round_1_sig_str_attempted,
            r_stats.round_1_sig_str_absorbed AS r_round_1_sig_str_absorbed,
            r_stats.round_1_sig_str_evaded AS r_round_1_sig_str_evaded,
            r_stats.round_2_sig_str_landed AS r_round_2_sig_str_landed,
            r_stats.round_2_sig_str_attempted AS r_round_2_sig_str_attempted,
            r_stats.round_2_sig_str_absorbed AS r_round_2_sig_str_absorbed,
            r_stats.round_2_sig_str_evaded AS r_round_2_sig_str_evaded,
            r_stats.round_3_sig_str_landed AS r_round_3_sig_str_landed,
            r_stats.round_3_sig_str_attempted AS r_round_3_sig_str_attempted,
            r_stats.round_3_sig_str_absorbed AS r_round_3_sig_str_absorbed,
            r_stats.round_3_sig_str_evaded AS r_round_3_sig_str_evaded,
            r_stats.round_4_sig_str_landed AS r_round_4_sig_str_landed,
            r_stats.round_4_sig_str_attempted AS r_round_4_sig_str_attempted,
            r_stats.round_4_sig_str_absorbed AS r_round_4_sig_str_absorbed,
            r_stats.round_4_sig_str_evaded AS r_round_4_sig_str_evaded,
            r_stats.round_5_sig_str_landed AS r_round_5_sig_str_landed,
            r_stats.round_5_sig_str_attempted AS r_round_5_sig_str_attempted,
            r_stats.round_5_sig_str_absorbed AS r_round_5_sig_str_absorbed,
            r_stats.round_5_sig_str_evaded AS r_round_5_sig_str_evaded,
            r_stats.fight_time AS r_fight_time,
            r_stats.opponent_fight_time AS r_opponent_fight_time,
            r_stats.reversals AS r_reversals,

            b_stats.age AS b_age,
            b_stats.wins AS b_wins,
            b_stats.losses AS b_losses,
            b_stats.draws AS b_draws,
            b_fighter.stance AS b_stance,
            b_fighter.height AS b_height,
            b_fighter.reach AS b_reach,
            b_stats.opponent_wins AS b_opponent_wins,
            b_stats.opponent_loses AS b_opponent_loses,
            b_stats.championship_fights AS b_championship_fights,
            b_stats.sig_str_landed AS b_sig_str_landed,
            b_stats.sig_str_attempted AS b_sig_str_attempted,
            b_stats.sig_str_absorbed AS b_sig_str_absorbed,
            b_stats.sig_str_evaded AS b_sig_str_evaded,
            b_stats.head_landed AS b_head_landed,
            b_stats.head_attempted AS b_head_attempted,
            b_stats.head_absorbed AS b_head_absorbed,
            b_stats.head_evaded AS b_head_evaded,
            b_stats.body_landed AS b_body_landed,
            b_stats.body_attempted AS b_body_attempted,
            b_stats.body_absorbed AS b_body_absorbed,
            b_stats.body_evaded AS b_body_evaded,
            b_stats.legs_landed AS b_legs_landed,
            b_stats.legs_attempted AS b_legs_attempted,
            b_stats.legs_absorbed AS b_legs_absorbed,
            b_stats.legs_evaded AS b_legs_evaded,
            b_stats.distance_landed AS b_distance_landed,
            b_stats.distance_attempted AS b_distance_attempted,
            b_stats.distance_absorbed AS b_distance_absorbed,
            b_stats.distance_evaded AS b_distance_evaded,
            b_stats.clinch_landed AS b_clinch_landed,
            b_stats.clinch_attempted AS b_clinch_attempted,
            b_stats.clinch_absorbed AS b_clinch_absorbed,
            b_stats.clinch_evaded AS b_clinch_evaded,
            b_stats.ground_landed AS b_ground_landed,
            b_stats.ground_attempted AS b_ground_attempted,
            b_stats.ground_absorbed AS b_ground_absorbed,
            b_stats.ground_evaded AS b_ground_evaded,
            b_stats.total_str_landed AS b_total_str_landed,
            b_stats.total_str_attempted AS b_total_str_attempted,
            b_stats.total_str_absorbed AS b_total_str_absorbed,
            b_stats.total_str_evaded AS b_total_str_evaded,
            b_stats.TD_landed AS b_TD_landed,
            b_stats.TD_attempted AS b_TD_attempted,
            b_stats.TD_absorbed AS b_TD_absorbed,
            b_stats.TD_evaded AS b_TD_evaded,
            b_stats.KD_landed AS b_KD_landed,
            b_stats.KD_absorbed AS b_KD_absorbed,
            b_stats.subs_attempted AS b_subs_attempted,
            b_stats.subs_evaded AS b_subs_evaded,
            b_stats.ctrl_time AS b_ctrl_time,
            b_stats.opponent_ctrl_time AS b_opponent_ctrl_time,
            b_stats.round_1_sig_str_landed AS b_round_1_sig_str_landed,
            b_stats.round_1_sig_str_attempted AS b_round_1_sig_str_attempted,
            b_stats.round_1_sig_str_absorbed AS b_round_1_sig_str_absorbed,
            b_stats.round_1_sig_str_evaded AS b_round_1_sig_str_evaded,
            b_stats.round_2_sig_str_landed AS b_round_2_sig_str_landed,
            b_stats.round_2_sig_str_attempted AS b_round_2_sig_str_attempted,
            b_stats.round_2_sig_str_absorbed AS b_round_2_sig_str_absorbed,
            b_stats.round_2_sig_str_evaded AS b_round_2_sig_str_evaded,
            b_stats.round_3_sig_str_landed AS b_round_3_sig_str_landed,
            b_stats.round_3_sig_str_attempted AS b_round_3_sig_str_attempted,
            b_stats.round_3_sig_str_absorbed AS b_round_3_sig_str_absorbed,
            b_stats.round_3_sig_str_evaded AS b_round_3_sig_str_evaded,
            b_stats.round_4_sig_str_landed AS b_round_4_sig_str_landed,
            b_stats.round_4_sig_str_attempted AS b_round_4_sig_str_attempted,
            b_stats.round_4_sig_str_absorbed AS b_round_4_sig_str_absorbed,
            b_stats.round_4_sig_str_evaded AS b_round_4_sig_str_evaded,
            b_stats.round_5_sig_str_landed AS b_round_5_sig_str_landed,
            b_stats.round_5_sig_str_attempted AS b_round_5_sig_str_attempted,
            b_stats.round_5_sig_str_absorbed AS b_round_5_sig_str_absorbed,
            b_stats.round_5_sig_str_evaded AS b_round_5_sig_str_evaded,
            b_stats.fight_time AS b_fight_time,
            b_stats.opponent_fight_time AS b_opponent_fight_time,
            b_stats.reversals AS b_reversals
            FROM
                total_fight_stats r_stats
            LEFT JOIN
                total_fight_stats b_stats ON b_stats.id = %(b_total_stats_id)s
            LEFT JOIN 
                fighters r_fighter ON %(r_fighter_id)s = r_fighter.id
            LEFT JOIN 
                fighters b_fighter ON %(b_fighter_id)s = b_fighter.id
            WHERE 
                r_stats.id = %(r_total_stats_id)s;
        """

        # Execute the query with the provided data
        cursor.execute(query, { 
            'r_fighter_id': r_fighter_id, 
            'b_fighter_id': b_fighter_id, 
            'r_total_stats_id': r_total_stats_id, 
            'b_total_stats_id': b_total_stats_id 
        })
        total_stats = cursor.fetchone()
        conn.commit()
        # print("Tables joined successfully: ", total_stats)
        return total_stats
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def convert_weight_class_to_weight(weight_class_data):
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
            weight = 140

    weight_class_data['weight'] = weight
    # print("fight data: {}".format(weight_class_data))

def scrape_fighter_names():
    ufc_card_url = 'http://ufcstats.com/event-details/a9df5ae20a97b090'
    fights = get_soup(ufc_card_url)
    #print(fights)
    fight_table_cells = fights.find('td', {'class': 'b-fight-details__table-col l-page_align_left'}, partial=False, mode='all')
    # print(fight_table_cells)
    grouped_cells = [fight_table_cells[i:i + 3] for i in range(0, len(fight_table_cells), 3)]
    # print(grouped_cells[0])
    fights_list = []
    for fight_detail_columns in grouped_cells:
        weight_class_data = {}
        # print(fight)
        # print("length: ", len(fight_detail_columns))
        fighter_names_container = fight_detail_columns[0]
        fighter_names = fighter_names_container.find('a', mode='all')
        weight_class_data['r_fighter_name'] = fighter_names[0].strip()
        weight_class_data['b_fighter_name'] = fighter_names[1].strip()
       
        weight_class = fight_detail_columns[1].text
        if weight_class.find('Women') != -1:
            weight_class = weight_class.split(' ')[1]
        
        weight_class_data['weight_class'] = weight_class.lower()
        # print('weight class data: {}'.format(weight_class_data))
        convert_weight_class_to_weight(weight_class_data)
        fights_list.append(weight_class_data)
    return fights_list

def fill_in_missing_columns(data_dummies):
    if 'gender_female' not in data_dummies:
        # print("No female fighters")
        data_dummies['gender_female'] = (0,) * len(data_dummies)
    if 'r_stance_Orthodox' not in data_dummies:
        data_dummies['r_stance_Orthodox'] = (0,) * len(data_dummies)
    if 'r_stance_Southpaw' not in data_dummies:
        data_dummies['r_stance_Southpaw'] = (0,) * len(data_dummies)
    if 'r_stance_Open Stance' not in data_dummies:
        data_dummies['r_stance_Open Stance'] = (0,) * len(data_dummies)
    if 'r_stance__Switch' not in data_dummies:
        data_dummies['r_stance_Switch'] = (0,) * len(data_dummies)
    if 'b_stance_Orthodox' not in data_dummies:
        data_dummies['b_stance_Orthodox'] = (0,) * len(data_dummies)
    if 'b_stance_Open Stance' not in data_dummies:
        data_dummies['b_stance_Open Stance'] = (0,) * len(data_dummies)
    if 'b_stance_Sideways' not in data_dummies:
        data_dummies['b_stance_Sideways'] = (0,) * len(data_dummies)
    if 'b_stance_Switch' not in data_dummies:
        data_dummies['b_stance_Switch'] = (0,) * len(data_dummies)
    if 'b_stance_Southpaw' not in data_dummies:
        data_dummies['b_stance_Southpaw'] = (0,) * len(data_dummies)

def predict_upcoming_fights():
    fights = scrape_fighter_names() # r_fighter_name, b_fighter_name, weight_class
    # print('fights', fights)
    upcoming_fights = []
    predictable_fights = []
    # print("details length", len(fights))
    for fight in fights:
        r_fighter_id, b_fighter_id = get_fighter_ids(fight['r_fighter_name'], fight['b_fighter_name'])
        if r_fighter_id is not None and b_fighter_id is not None:
            r_total_stats_id = get_last_total_stats(r_fighter_id)
            b_total_stats_id = get_last_total_stats(b_fighter_id)
            if r_total_stats_id is not None and b_total_stats_id is not None:
                joined_fight_info = join_tables(r_fighter_id, b_fighter_id, r_total_stats_id, b_total_stats_id)
                upcoming_fight = (fight['weight'],) + joined_fight_info
                # print("fight_info length: ", len(upcoming_fight))
                upcoming_fights.append(upcoming_fight)
                predictable_fights.append(fight)
    # print("upcoming figihts: ", len(upcoming_fights))
    feature_cols = ["weight","gender","r_age","r_wins","r_losses","r_draws","r_stance","r_height","r_reach","r_opponent_wins","r_opponent_loses","r_championship_fights","r_sig_str_landed","r_sig_str_attempted","r_sig_str_absorbed","r_sig_str_evaded","r_head_landed","r_head_attempted","r_head_absorbed","r_head_evaded","r_body_landed","r_body_attempted","r_body_absorbed","r_body_evaded","r_legs_landed","r_legs_attempted","r_legs_absorbed","r_legs_evaded","r_distance_landed","r_distance_attempted","r_distance_absorbed","r_distance_evaded","r_clinch_landed","r_clinch_attempted","r_clinch_absorbed","r_clinch_evaded","r_ground_landed","r_ground_attempted","r_ground_absorbed","r_ground_evaded","r_total_str_landed","r_total_str_attempted","r_total_str_absorbed","r_total_str_evaded","r_td_landed","r_td_attempted","r_td_absorbed","r_td_evaded","r_kd_landed","r_kd_absorbed","r_subs_attempted","r_subs_evaded","r_ctrl_time","r_opponent_ctrl_time","r_round_1_sig_str_landed","r_round_1_sig_str_attempted","r_round_1_sig_str_absorbed","r_round_1_sig_str_evaded","r_round_2_sig_str_landed","r_round_2_sig_str_attempted","r_round_2_sig_str_absorbed","r_round_2_sig_str_evaded","r_round_3_sig_str_landed","r_round_3_sig_str_attempted","r_round_3_sig_str_absorbed","r_round_3_sig_str_evaded","r_round_4_sig_str_landed","r_round_4_sig_str_attempted","r_round_4_sig_str_absorbed","r_round_4_sig_str_evaded","r_round_5_sig_str_landed","r_round_5_sig_str_attempted","r_round_5_sig_str_absorbed","r_round_5_sig_str_evaded","r_fight_time","r_opponent_fight_time","r_reversals","b_age","b_wins","b_losses","b_draws","b_stance","b_height","b_reach","b_opponent_wins","b_opponent_loses","b_championship_fights","b_sig_str_landed","b_sig_str_attempted","b_sig_str_absorbed","b_sig_str_evaded","b_head_landed","b_head_attempted","b_head_absorbed","b_head_evaded","b_body_landed","b_body_attempted","b_body_absorbed","b_body_evaded","b_legs_landed","b_legs_attempted","b_legs_absorbed","b_legs_evaded","b_distance_landed","b_distance_attempted","b_distance_absorbed","b_distance_evaded","b_clinch_landed","b_clinch_attempted","b_clinch_absorbed","b_clinch_evaded","b_ground_landed","b_ground_attempted","b_ground_absorbed","b_ground_evaded","b_total_str_landed","b_total_str_attempted","b_total_str_absorbed","b_total_str_evaded","b_td_landed","b_td_attempted","b_td_absorbed","b_td_evaded","b_kd_landed","b_kd_absorbed","b_subs_attempted","b_subs_evaded","b_ctrl_time","b_opponent_ctrl_time","b_round_1_sig_str_landed","b_round_1_sig_str_attempted","b_round_1_sig_str_absorbed","b_round_1_sig_str_evaded","b_round_2_sig_str_landed","b_round_2_sig_str_attempted","b_round_2_sig_str_absorbed","b_round_2_sig_str_evaded","b_round_3_sig_str_landed","b_round_3_sig_str_attempted","b_round_3_sig_str_absorbed","b_round_3_sig_str_evaded","b_round_4_sig_str_landed","b_round_4_sig_str_attempted","b_round_4_sig_str_absorbed","b_round_4_sig_str_evaded","b_round_5_sig_str_landed","b_round_5_sig_str_attempted","b_round_5_sig_str_absorbed","b_round_5_sig_str_evaded","b_fight_time","b_opponent_fight_time","b_reversals"]
    # print("columns length: ", len(feature_cols))
    df = pd.DataFrame(upcoming_fights, columns = feature_cols)
    data = df.loc[:, feature_cols]
    data_dummies = pd.get_dummies(data)
    #  print("dummies length", data_dummies.columns)
    # for col in data_dummies.columns:
    #    print(col)
    fill_in_missing_columns(data_dummies)

    predictable_data = data_dummies.values
    # print("predictable_data: ", predictable_data)
    loaded_model = pickle.load(open('./models/trained/gradient_boosted_classifier-2023-12-16.sav', 'rb'))
    results = loaded_model.predict(predictable_data)
    confidence_levels = loaded_model.predict_proba(predictable_data)
    # print(result)
    print("TRAIN TEST SPLIT GRADIENT BOOSTED")
    for i, fight in enumerate(predictable_fights):
        print(f"Fight: {fight['r_fighter_name']} vs {fight['b_fighter_name']}")
        winner = fight['r_fighter_name']
        if results[i-1] == 1:
            winner = fight['b_fighter_name']

        formatted_numbers = ["{:.3f}".format(x) for x in confidence_levels[i-1]]
        print(f"Winner: {winner}, Confidence: {float(max(formatted_numbers)) * 100:.1f}% \n") 

predict_upcoming_fights()

