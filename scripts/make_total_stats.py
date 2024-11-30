import psycopg2
from datetime import date

# Sort fight by date earliest -> latest
# For each fight find the last time someone fought (by fighter id)
    # find last fight_total_stat row
    # add last_total_stats + current fight stats to make current_total_stats


def get_last_fight_data(fighter_id, date):
    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port= '5432'
        )
        cursor = conn.cursor()

        query = """
            SELECT *
            FROM fights
            WHERE (r_fighter_id = %(fighter_id)s OR b_fighter_id = %(fighter_id)s)
                AND date = (
                    SELECT MAX(date)
                    FROM fights
                    WHERE (r_fighter_id = %(fighter_id)s OR b_fighter_id = %(fighter_id)s) AND 
                    date < %(date)s AND 
                    r_fighter_id IS NOT NULL AND b_fighter_id IS NOT NULL AND
                    r_total_stats_id IS NOT NULL AND
                    b_total_stats_id IS NOT NULL
                );
        """

        # Execute the query with the provided data
        data = {'fighter_id': fighter_id, 'date': date }
        cursor.execute(query, data)
        print("Getting last fight: {} {}".format(fighter_id, date))
        fight_info = cursor.fetchone()
        print("Last fight {}".format(fight_info))
        # Commit the changes to the database
        conn.commit()
        # print("last fight: {}".format(fight_info))
        return fight_info
    except (Exception, psycopg2.Error) as error:
        print("GET LAST FIGHT DATA")
        print("Error while selecting row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def get_last_total_stats(total_stats_id):
    if total_stats_id is None:
        return None
    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port= '5432'
        )
        cursor = conn.cursor()

        query = """
            SELECT * FROM total_fight_stats WHERE id = %(total_stats_id)s
        """

        data = {'total_stats_id': total_stats_id}
        # Execute the query with the provided data
        cursor.execute(query, data)
        fight_info = cursor.fetchone()
        # Commit the changes to the database
        conn.commit()
        return fight_info
    except (Exception, psycopg2.Error) as error:
        print("GET LAST TOTAL STATS")
        print("Error while selecting row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def get_fight_data():
    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port= '5432'
        )
        cursor = conn.cursor()

        query = """
            SELECT * from fights 
            WHERE r_total_stats_id IS NULL AND b_total_stats_id IS NULL AND date > '2023-10-07'
            ORDER BY date ASC
        """

        # Execute the query with the provided data
        cursor.execute(query)
        fight_info = cursor.fetchall()
        # Commit the changes to the database
        conn.commit()
        return fight_info
    except (Exception, psycopg2.Error) as error:
        print("GET FIGHT DATA")
        print("Error while selecting row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def get_fight_stats(fight_stat_id):
    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port= '5432'
        )
        cursor = conn.cursor()

        query = """
            SELECT * FROM fight_stats WHERE id = %(fight_stat_id)s
        """

        data = {'fight_stat_id': fight_stat_id}
        # Execute the query with the provided data
        cursor.execute(query, data)
        fight_info = cursor.fetchone()
        # Commit the changes to the database
        conn.commit()
        print("id: {} fight_info: {}".format(fight_stat_id, fight_info))
        return fight_info
    except (Exception, psycopg2.Error) as error:
        print("GET FIGHT STATS")
        print("Error while selecting row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def get_fighter_data(fighter_id):
    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port= '5432'
        )
        cursor = conn.cursor()

        query = """
            SELECT * from fighters WHERE id = %(fighter_id)s
        """

        data = {'fighter_id': fighter_id}
        # Execute the query with the provided data
        cursor.execute(query, data)
        fight_info = cursor.fetchone()
        # Commit the changes to the database
        conn.commit()
        return fight_info
    except (Exception, psycopg2.Error) as error:
        print("GET FIGHTER DATA")
        print("Error while selecting row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def save_cumulative_fight_data(data):
    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port= '5432'
        )
        cursor = conn.cursor()

        query = """
            INSERT INTO total_fight_stats (
                name, age, fighter_id, fight_id, wins, losses, draws, 
                opponent_wins, opponent_loses, championship_fights,
                sig_str_landed, sig_str_attempted, sig_str_absorbed, sig_str_evaded, 
                head_landed, head_attempted, head_absorbed, head_evaded, 
                body_landed, body_attempted, body_absorbed, body_evaded, 
                legs_landed, legs_attempted, legs_absorbed, legs_evaded, 
                distance_landed, distance_attempted, distance_absorbed, distance_evaded, 
                clinch_landed, clinch_attempted, clinch_absorbed, clinch_evaded, 
                ground_landed, ground_attempted, ground_absorbed, ground_evaded, 
                total_str_landed, total_str_attempted, total_str_absorbed, total_str_evaded,
                td_landed, td_attempted, td_absorbed, td_evaded, 
                kd_landed, kd_absorbed, subs_attempted, subs_evaded, 
                ctrl_time, opponent_ctrl_time, fight_time, opponent_fight_time, reversals,
                round_1_sig_str_landed, round_1_sig_str_attempted, round_1_sig_str_absorbed, round_1_sig_str_evaded,
                round_2_sig_str_landed, round_2_sig_str_attempted, round_2_sig_str_absorbed, round_2_sig_str_evaded,
                round_3_sig_str_landed, round_3_sig_str_attempted, round_3_sig_str_absorbed, round_3_sig_str_evaded,
                round_4_sig_str_landed, round_4_sig_str_attempted, round_4_sig_str_absorbed, round_4_sig_str_evaded,
                round_5_sig_str_landed, round_5_sig_str_attempted, round_5_sig_str_absorbed, round_5_sig_str_evaded
            )
            VALUES (
                %(name)s, %(age)s, %(fighter_id)s, %(fight_id)s, %(wins)s, %(losses)s, %(draws)s, 
                %(opponent_wins)s, %(opponent_loses)s, %(championship_fights)s,
                %(sig_str_landed)s, %(sig_str_attempted)s, %(sig_str_absorbed)s, %(sig_str_evaded)s,
                %(head_landed)s, %(head_attempted)s, %(head_absorbed)s, %(head_evaded)s,
                %(body_landed)s, %(body_attempted)s, %(body_absorbed)s, %(body_evaded)s,
                %(legs_landed)s, %(legs_attempted)s, %(legs_absorbed)s, %(legs_evaded)s,
                %(distance_landed)s, %(distance_attempted)s, %(distance_absorbed)s, %(distance_evaded)s,
                %(clinch_landed)s, %(clinch_attempted)s, %(clinch_absorbed)s, %(clinch_evaded)s,
                %(ground_landed)s, %(ground_attempted)s, %(ground_absorbed)s, %(ground_evaded)s,
                %(total_str_landed)s, %(total_str_attempted)s, %(total_str_absorbed)s, %(total_str_evaded)s,
                %(td_landed)s, %(td_attempted)s, %(td_absorbed)s, %(td_evaded)s,
                %(kd_landed)s, %(kd_absorbed)s, %(subs_attempted)s, %(subs_evaded)s,
                %(ctrl_time)s, %(opponent_ctrl_time)s, %(fight_time)s, %(opponent_fight_time)s, %(reversals)s,
                %(round_1_sig_str_landed)s, %(round_1_sig_str_attempted)s, %(round_1_sig_str_absorbed)s, %(round_1_sig_str_evaded)s,
                %(round_2_sig_str_landed)s, %(round_2_sig_str_attempted)s, %(round_2_sig_str_absorbed)s, %(round_2_sig_str_evaded)s,
                %(round_3_sig_str_landed)s, %(round_3_sig_str_attempted)s, %(round_3_sig_str_absorbed)s, %(round_3_sig_str_evaded)s,
                %(round_4_sig_str_landed)s, %(round_4_sig_str_attempted)s, %(round_4_sig_str_absorbed)s, %(round_4_sig_str_evaded)s,
                %(round_5_sig_str_landed)s, %(round_5_sig_str_attempted)s, %(round_5_sig_str_absorbed)s, %(round_5_sig_str_evaded)s
            )
            RETURNING id;
        """

        # Execute the query with the provided data
        cursor.execute(query, data)
        total_stats_id = cursor.fetchone()[0]
        # Commit the changes to the database
        conn.commit()
        return total_stats_id
    except (Exception, psycopg2.Error) as error:
        print("Adding total stats")
        print("Error while inserting row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def save_total_stats_ids(data):
    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port='5432'
        )
        cursor = conn.cursor()
        # Construct the SQL query with dynamic values
        query = """
            UPDATE fights
            SET r_total_stats_id = %(r_total_stats_id)s, b_total_stats_id = %(b_total_stats_id)s
            WHERE id = %(fight_id)s;
        """

        # Execute the query with the provided data
        cursor.execute(query, data)
        conn.commit()
        print("Saved total stats ids successfully")
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def fight_stats_tuple_to_dict(fight_stats_tuple):
    column_names = [
        "name","fighter_id","sig_str_landed","sig_str_attempted","head_landed","head_attempted",
        "body_landed","body_attempted","legs_landed","legs_attempted","distance_landed","distance_attempted",
        "clinch_landed","clinch_attempted","ground_landed","ground_attempted","total_str_landed",
        "total_str_attempted","td_landed","td_attempted","kd_landed","subs_attempted","ctrl_time",
        "round_1_sig_str_landed","round_1_sig_str_attempted","round_2_sig_str_landed","round_2_sig_str_attempted",
        "round_3_sig_str_landed","round_3_sig_str_attempted","round_4_sig_str_landed","round_4_sig_str_attempted",
        "round_5_sig_str_landed","round_5_sig_str_attempted","id","reversals"
    ]

    # Using zip() to combine the column names and the tuple values into a dictionary
    return dict(zip(column_names, fight_stats_tuple))

def initialize_first_total_stats():
    initial_values = (
        None,  # id
        "",    # name
        0,     # age
        None,  # fighter_id
        None,  # fight_id
        0,     # wins
        0,     # losses
        0,     # draws
        0,     # championship_fights
        0,     # sig_str_landed
        0,     # sig_str_attempted
        0,     # sig_str_absorbed
        0,     # sig_str_evaded
        0,     # head_landed
        0,     # head_attempted
        0,     # head_absorbed
        0,     # head_evaded
        0,     # body_landed
        0,     # body_attempted
        0,     # body_absorbed
        0,     # body_evaded
        0,     # legs_landed
        0,     # legs_attempted
        0,     # legs_absorbed
        0,     # legs_evaded
        0,     # distance_landed
        0,     # distance_attempted
        0,     # distance_absorbed
        0,     # distance_evaded
        0,     # clinch_landed
        0,     # clinch_attempted
        0,     # clinch_absorbed
        0,     # clinch_evaded
        0,     # ground_landed
        0,     # ground_attempted
        0,     # ground_absorbed
        0,     # ground_evaded
        0,     # total_strike_landed
        0,     # total_strikes_attempted
        0,     # total_strikes_absorbed
        0,     # total_strikes_evaded
        0,     # td_landed
        0,     # td_attempted
        0,     # td_absorbed
        0,     # td_evaded
        0,     # round_1_sig_str_landed
        0,     # round_1_sig_str_attempted
        0,     # round_1_sig_str_absorbed
        0,     # round_1_sig_str_evaded
        0,     # round_2_sig_str_landed
        0,     # round_2_sig_str_attempted
        0,     # round_2_sig_str_absorbed
        0,     # round_2_sig_str_evaded
        0,     # round_3_sig_str_landed
        0,     # round_3_sig_str_attempted
        0,     # round_3_sig_str_absorbed
        0,     # round_3_sig_str_evaded
        0,     # round_4_sig_str_landed
        0,     # round_4_sig_str_attempted
        0,     # round_4_sig_str_absorbed
        0,     # round_4_sig_str_evaded
        0,     # round_5_sig_str_landed
        0,     # round_5_sig_str_attempted
        0,     # round_5_sig_str_absorbed
        0,     # round_5_sig_str_evaded
        0,     # ctrl_time
        0,     # opponent_ctrl_time
        0,     # subs_attempted
        0,     # subs_evaded
        0,     # kd_landed
        0,     # kd_absorbed
        0,     # opponent wins
        0,     # opponent loses
        0,     # fight_time
        0,     # opponent_fight_time
        0,     # reversals
    )

    return initial_values

def fight_tuple_to_dict(fight_tuple):
    if fight_tuple is None:
        return None

    column_names = [
        "id","r_fighter_id","b_fighter_id","location","date","championship_fight","winner","win_method",
        "win_method_details","number_of_rounds","finish_round","finish_time","referee","r_fight_stats_id",
        "b_fight_stats_id","r_name","b_name","total_fight_time","weight","fight_odds_id","r_fighter_odds",
        "b_fighter_odds", "r_total_stats_id",  "b_total_stats_id"
    ]

    # Using zip() to combine the column names and the tuple values into a dictionary
    return dict(zip(column_names, fight_tuple))

def update_new_total_stats(category, r_laae, b_laae, r_fight_stats_dict, b_fight_stats_dict, r_new_total_stats, b_new_total_stats):
    r_landed, r_attempted, r_absorbed, r_evaded = r_laae
    b_landed, b_attempted, b_absorbed, b_evaded = b_laae

    print("category: {}".format(category))
    print("r_fight_stats_dict: {}".format(r_fight_stats_dict))
    print("r_landed: {}".format(r_landed))

    r_new_total_stats[category + "_landed"] = r_fight_stats_dict[category + "_landed"] + r_landed
    r_new_total_stats[category + "_attempted"] = r_fight_stats_dict[category + "_attempted"] + r_attempted
    r_new_total_stats[category + "_absorbed"] = r_absorbed + b_fight_stats_dict[category + "_landed"]
    r_new_total_stats[category + "_evaded"] = r_evaded + b_fight_stats_dict[category + "_attempted"] - b_fight_stats_dict[category + "_landed"]

    b_new_total_stats[category + "_landed"] = b_fight_stats_dict[category + "_landed"] + b_landed
    b_new_total_stats[category + "_attempted"] = b_fight_stats_dict[category + "_attempted"] + b_attempted
    b_new_total_stats[category + "_absorbed"] = b_absorbed + r_fight_stats_dict[category + "_landed"]
    b_new_total_stats[category + "_evaded"] = b_evaded + r_fight_stats_dict[category + "_attempted"] - r_fight_stats_dict[category + "_landed"]

def update_win_loss_draw(r_new_total_stats, b_new_total_stats, r_wld, b_wld, fight_dict, r_last_total_stats_tuple, b_last_total_stats_tuple):
    r_opponent_wins = r_last_total_stats_tuple[71]
    r_opponent_losses = r_last_total_stats_tuple[72]
    b_opponent_wins = b_last_total_stats_tuple[71]
    b_opponent_losses = b_last_total_stats_tuple[72]

    r_new_total_stats['opponent_wins'] = r_opponent_wins + b_wld[0]
    r_new_total_stats['opponent_loses'] = r_opponent_losses + b_wld[1]
    b_new_total_stats['opponent_wins'] = b_opponent_wins + r_wld[0]
    b_new_total_stats['opponent_loses'] = b_opponent_losses + r_wld[1]
    print('r_wld: {}'.format(r_wld))
    print('b_wld: {}'.format(b_wld))

    if fight_dict['winner'] == '':
        r_new_total_stats['wins'] = r_wld[0]
        r_new_total_stats['losses'] = r_wld[1]
        r_new_total_stats['draws'] = r_wld[2] + 1

        b_new_total_stats['wins'] = b_wld[0]
        b_new_total_stats['losses'] = b_wld[1]
        b_new_total_stats['draws'] = b_wld[2] + 1

    elif fight_dict['winner'] == fight_dict['r_name']:
        r_new_total_stats['wins'] = r_wld[0] + 1
        r_new_total_stats['losses'] = r_wld[1]
        r_new_total_stats['draws'] = r_wld[2]

        b_new_total_stats['wins'] = b_wld[0]
        b_new_total_stats['losses'] = b_wld[1] + 1
        b_new_total_stats['draws'] = b_wld[2]

    elif fight_dict['winner'] == fight_dict['b_name']:
        r_new_total_stats['wins'] = r_wld[0]
        r_new_total_stats['losses'] = r_wld[1] + 1
        r_new_total_stats['draws'] = r_wld[2]

        b_new_total_stats['wins'] = b_wld[0] + 1
        b_new_total_stats['losses'] = b_wld[1]
        b_new_total_stats['draws'] = b_wld[2]
    
    else:
        print("INCORRECT WINNER VALUE {}".format(fight_dict['winner']))

def get_all_data(fight_dict):
    r_fighter_id = fight_dict['r_fighter_id']
    b_fighter_id = fight_dict['b_fighter_id']

    r_fighter_last_fight = fight_tuple_to_dict(get_last_fight_data(r_fighter_id, fight_dict['date']))
    print("r_fighter_last_fight: {}".format(r_fighter_last_fight))
         
    b_fighter_last_fight = fight_tuple_to_dict(get_last_fight_data(b_fighter_id, fight_dict['date']))
    print("b_fighter_last_fight: {}".format(r_fighter_last_fight))

    r_fighter = get_fighter_data(r_fighter_id)
    b_fighter = get_fighter_data(b_fighter_id)
    r_last_total_stats_tuple = None
    b_last_total_stats_tuple = None

    if r_fighter_last_fight is not None:
        attribute = 'r_total_stats_id'
        if r_fighter[1] == r_fighter_last_fight['b_name']:
            attribute = 'b_total_stats_id'
        # print("r_last_fight: {}".format(r_fighter_last_fight))
        r_last_total_stats_tuple = get_last_total_stats(r_fighter_last_fight[attribute])
    if b_fighter_last_fight is not None:
        attribute = 'b_total_stats_id'
        if b_fighter[1] == b_fighter_last_fight['r_name']:
            attribute = 'r_total_stats_id'
        # print("b_last_fight: {}".format(b_fighter_last_fight))
        b_last_total_stats_tuple = get_last_total_stats(b_fighter_last_fight[attribute])

    return r_fighter, b_fighter, r_last_total_stats_tuple, b_last_total_stats_tuple 

def update_total_stats_basic_info(new_total_stats, last_total_stats_tuple, fighter, fight_dict):
    new_total_stats['name'] = fighter[1]
    # print("fight_dict: {}".format(fight_dict['date']))
    # print("fighter birth: {}".format(fighter[6]))
    delta = fight_dict['date'] - fighter[6]
    new_total_stats['age'] = delta.days
    new_total_stats['fighter_id'] = fighter[0]
    new_total_stats['fight_id'] = fight_dict['id']
    new_total_stats['championship_fights'] = last_total_stats_tuple[8] + int(fight_dict['championship_fight'])

def update_total_fight_stats(fight, r_new_total_stats, b_new_total_stats, r_last_total_stats_tuple, b_last_total_stats_tuple):
    stat_categories = ['sig_str', 'head', 'body', 'legs', 'distance', 'clinch', 'ground', 'total_str', 'td', 'round_1_sig_str', 'round_2_sig_str', 'round_3_sig_str', 'round_4_sig_str', 'round_5_sig_str']

    r_fight_stats = get_fight_stats(fight['r_fight_stats_id'])
    b_fight_stats = get_fight_stats(fight['b_fight_stats_id'])
    # print('r_fight_stats: {}'.format(r_fight_stats))
    r_fight_stats_dict = fight_stats_tuple_to_dict(r_fight_stats)
    b_fight_stats_dict = fight_stats_tuple_to_dict(b_fight_stats)

    # print("r_fight_stats_dict: {}".format(r_fight_stats_dict))
    for i in range(0, 14):
        tuple_idx = 4 * i + 9
        r_laae = [r_last_total_stats_tuple[tuple_idx], r_last_total_stats_tuple[tuple_idx+1], r_last_total_stats_tuple[tuple_idx+2], r_last_total_stats_tuple[tuple_idx+3]]
        b_laae = [b_last_total_stats_tuple[tuple_idx], b_last_total_stats_tuple[tuple_idx+1], b_last_total_stats_tuple[tuple_idx+2], b_last_total_stats_tuple[tuple_idx+3]]
        update_new_total_stats(stat_categories[i], r_laae, b_laae, r_fight_stats_dict, b_fight_stats_dict, r_new_total_stats, b_new_total_stats)     

    r_ctrl_time, r_opponent_ctrl_time = [r_last_total_stats_tuple[65], r_last_total_stats_tuple[66]]
    r_new_total_stats['ctrl_time'] = r_ctrl_time + int(r_fight_stats_dict['ctrl_time'])
    r_new_total_stats['opponent_ctrl_time'] = r_opponent_ctrl_time + int(b_fight_stats_dict['ctrl_time'])

    r_kd_landed, r_kd_absorbed = [r_last_total_stats_tuple[67], r_last_total_stats_tuple[68]]
    r_new_total_stats['kd_landed'] = r_kd_landed + r_fight_stats_dict['kd_landed']
    r_new_total_stats['kd_absorbed'] = r_kd_absorbed + b_fight_stats_dict['kd_landed']

    r_subs_attempted, r_subs_evaded = [r_last_total_stats_tuple[69], r_last_total_stats_tuple[70]]
    r_new_total_stats['subs_attempted'] = r_subs_attempted + r_fight_stats_dict['subs_attempted']
    r_new_total_stats['subs_evaded'] = r_subs_evaded + b_fight_stats_dict['subs_attempted']

    b_ctrl_time, b_opponent_ctrl_time = [b_last_total_stats_tuple[65], b_last_total_stats_tuple[66]]
    b_new_total_stats['ctrl_time'] = b_ctrl_time + int(b_fight_stats_dict['ctrl_time'])
    b_new_total_stats['opponent_ctrl_time'] = b_opponent_ctrl_time + int(r_fight_stats_dict['ctrl_time'])

    b_kd_landed, b_kd_absorbed = [b_last_total_stats_tuple[67], b_last_total_stats_tuple[68]]
    b_new_total_stats['kd_landed'] = b_kd_landed + b_fight_stats_dict['kd_landed']
    b_new_total_stats['kd_absorbed'] = b_kd_absorbed + r_fight_stats_dict['kd_landed']

    b_subs_attempted, b_subs_evaded = [b_last_total_stats_tuple[69], b_last_total_stats_tuple[70]]
    b_new_total_stats['subs_attempted'] = b_subs_attempted + b_fight_stats_dict['subs_attempted']
    b_new_total_stats['subs_evaded'] = b_subs_evaded + r_fight_stats_dict['subs_attempted']

    r_fight_time, r_opponent_fight_time = [r_last_total_stats_tuple[73], r_last_total_stats_tuple[74]]
    b_fight_time, b_opponent_fight_time = [b_last_total_stats_tuple[73], b_last_total_stats_tuple[74]]
    current_fight_time = (fight['finish_round'] - 1) * 300 + fight['finish_time'].hour * 60 + fight['finish_time'].minute
    
    r_new_total_stats['fight_time'] = r_fight_time + current_fight_time
    r_new_total_stats['opponent_fight_time'] = r_opponent_fight_time + b_fight_time

    b_new_total_stats['fight_time'] = b_fight_time + current_fight_time
    b_new_total_stats['opponent_fight_time'] = b_opponent_fight_time + r_fight_time

    r_reversals = r_last_total_stats_tuple[75]
    r_new_total_stats['reversals'] = r_reversals + r_fight_stats_dict['reversals']
    
    b_reversals = b_last_total_stats_tuple[75]
    b_new_total_stats['reversals'] = b_reversals + b_fight_stats_dict['reversals']

def get_total_fight_stats(fight):
    fight_dict = fight_tuple_to_dict(fight)
    r_fighter, b_fighter, r_last_total_stats_tuple, b_last_total_stats_tuple = get_all_data(fight_dict)
    
    # print("fight_dict: {}".format(fight_dict))
    # print("r_fighter: {}".format(r_fighter))
    # print("b_fighter: {}".format(b_fighter))

    if r_last_total_stats_tuple is None:
        print("r_last_total_stats_tuple is None")
        r_last_total_stats_tuple = initialize_first_total_stats()

    if b_last_total_stats_tuple is None:
        print("b_last_total_stats_tuple is None")
        b_last_total_stats_tuple = initialize_first_total_stats()

    # print("date: {}".format(fight_dict['date']))
    # print("r_last_total_stats_tuple: {}".format(r_last_total_stats_tuple))
    # print("b_last_total_stats_tuple: {}".format(b_last_total_stats_tuple))
    # breakpoint()
    r_new_total_stats = {}
    b_new_total_stats = {}

    update_total_stats_basic_info(r_new_total_stats, r_last_total_stats_tuple, r_fighter, fight_dict)
    update_total_stats_basic_info(b_new_total_stats, b_last_total_stats_tuple, b_fighter, fight_dict)

    r_wld = [r_last_total_stats_tuple[5], r_last_total_stats_tuple[6], r_last_total_stats_tuple[7]]
    b_wld = [b_last_total_stats_tuple[5], b_last_total_stats_tuple[6], b_last_total_stats_tuple[7]]
    update_win_loss_draw(r_new_total_stats, b_new_total_stats, r_wld, b_wld, fight_dict, r_last_total_stats_tuple, b_last_total_stats_tuple)
    print('fight', fight_dict)
    update_total_fight_stats(fight_dict, r_new_total_stats, b_new_total_stats, r_last_total_stats_tuple, b_last_total_stats_tuple)
    return r_new_total_stats, b_new_total_stats
    # print("r_new_total_stats: {}".format(r_new_total_stats))
    # print("b_new_total_stats: {}".format(b_new_total_stats))

def create_cumulative_fight_data():
    fights = get_fight_data()
    # print("fights: {}".format(fights))

    for fight in fights:
        print(fight)
        r_new_total_stats, b_new_total_stats = get_total_fight_stats(fight)

        # print("r_new_total_stats: {}".format(r_new_total_stats))
        # print("b_new_total_stats: {}".format(b_new_total_stats))
        
        r_total_stats_id = save_cumulative_fight_data(r_new_total_stats)
        b_total_stats_id = save_cumulative_fight_data(b_new_total_stats)
        # print("r_total_stats_id: {}".format(r_total_stats_id))
        # print("b_total_stats_id: {}".format(b_total_stats_id))
        save_total_stats_ids({ 'fight_id': fight[0], 'r_total_stats_id': r_total_stats_id , 'b_total_stats_id': b_total_stats_id })



create_cumulative_fight_data()

        




